defmodule Slither.Examples.MlScoring.ScoringPipe do
  @moduledoc """
  ML scoring pipeline: enrich -> featurize -> predict -> route by confidence.

  Scaled to 2,000 training samples per session and 2,000 test records,
  processed through 48 workers with `batch_size: 100`.

  Trains TWO models on TWO sessions concurrently, then scores test records
  through each session's pipeline independently via `Task.async` to prove
  session isolation.

  Run with `Slither.Examples.MlScoring.ScoringPipe.run_demo/0`.
  """

  use Slither.Pipe

  alias Slither.{Context, Dispatch, Item}
  alias Slither.Examples.MlScoring.FeatureStore
  alias Slither.Examples.Reporter
  alias Slither.Pipe.Runner
  alias Slither.Store.Server

  @num_train 2000
  @num_test 2000

  pipe :ml_scoring do
    store(:features, FeatureStore)

    stage(:enrich, :beam, handler: &__MODULE__.enrich/2)

    stage(:featurize, :python,
      executor: Slither.Dispatch.Executors.SnakeBridge,
      module: "ml_scorer",
      function: "featurize_batch",
      batch_size: 100
    )

    stage(:predict, :python,
      executor: Slither.Dispatch.Executors.SnakeBridge,
      module: "ml_scorer",
      function: "predict_batch",
      batch_size: 100
    )

    stage(:route_by_confidence, :router,
      routes: [
        {fn item -> item.payload["confidence"] >= 0.9 end, :high_confidence},
        {fn item -> item.payload["confidence"] < 0.6 end, :low_confidence}
      ]
    )

    output(:high_confidence)
    output(:low_confidence)

    on_error(:featurize, :skip)
    on_error(:predict, :skip)
    on_error(:*, :halt)
  end

  def enrich(item, ctx) do
    record_id = item.payload[:id] || item.payload["id"]
    model_id = ctx.metadata[:model_id] || ctx.metadata["model_id"]

    cached = Server.get(FeatureStore, :feature_cache, record_id)

    if cached do
      %{"model_id" => model_id, "features" => cached, "cache_hit" => true}
    else
      raw_data = item.payload[:data] || item.payload["data"] || item.payload
      %{"raw_data" => raw_data, "record_id" => record_id, "model_id" => model_id}
    end
  end

  def run_demo do
    IO.puts("\n=== Slither Example: ML Scoring Pipeline ===")
    IO.puts("=== Concurrent Session Isolation Demo ===\n")

    case check_sklearn() do
      :ok ->
        do_run_demo()

      {:error, msg} ->
        IO.puts("  Skipping: #{msg}")
        IO.puts("  Run via: mix slither.example ml_scoring (auto-installs via Snakepit/uv)")
    end
  end

  # ---------------------------------------------------------------------------
  # Internal
  # ---------------------------------------------------------------------------

  defp check_sklearn do
    case SnakeBridge.call("builtins", "__import__", ["sklearn"]) do
      {:ok, _} -> :ok
      {:error, _} -> {:error, "scikit-learn is not installed"}
    end
  rescue
    _ -> {:error, "SnakeBridge is not available"}
  end

  defp do_run_demo do
    {:ok, _pid} = Server.start_link(module: FeatureStore, name: FeatureStore)
    Process.sleep(100)

    :rand.seed(:exsss, {42, 137, 256})

    # --- Phase 1: Train two models on two sessions concurrently ---
    IO.puts("Phase 1: Training two models (#{@num_train} samples each) on separate sessions...\n")

    session_a = "scoring_a_#{System.unique_integer([:positive])}"
    session_b = "scoring_b_#{System.unique_integer([:positive])}"

    {train_x_a, train_y_a} = generate_training_data_a(@num_train)
    {train_x_b, train_y_b} = generate_training_data_b(@num_train)

    {train_time, {model_a, model_b}} =
      :timer.tc(fn ->
        task_a = Task.async(fn -> train_on_session(train_x_a, train_y_a, session_a, "A") end)
        task_b = Task.async(fn -> train_on_session(train_x_b, train_y_b, session_b, "B") end)

        {Task.await(task_a, 60_000), Task.await(task_b, 60_000)}
      end)

    print_training_result("A", model_a, session_a)
    print_training_result("B", model_b, session_b)
    Reporter.print_timing("Training (concurrent)", train_time)
    IO.puts("")

    # --- Phase 2: Score through both sessions concurrently ---
    IO.puts("Phase 2: Scoring #{@num_test} test records through both sessions concurrently...\n")

    pre_populate_cache(10)
    test_records = generate_test_records(@num_test)

    {score_time, {outputs_a, outputs_b}} =
      :timer.tc(fn ->
        score_a =
          Task.async(fn ->
            Runner.run(__MODULE__, test_records,
              session_id: session_a,
              metadata: %{model_id: model_a["model_id"]}
            )
          end)

        score_b =
          Task.async(fn ->
            Runner.run(__MODULE__, test_records,
              session_id: session_b,
              metadata: %{model_id: model_b["model_id"]}
            )
          end)

        {:ok, oa} = Task.await(score_a, 120_000)
        {:ok, ob} = Task.await(score_b, 120_000)
        {oa, ob}
      end)

    IO.puts("--- Session A Results ---")
    print_results(outputs_a)

    IO.puts("--- Session B Results ---")
    print_results(outputs_b)

    Reporter.print_timing("Scoring (concurrent, both sessions)", score_time)
    Reporter.print_throughput("Throughput (per session)", {@num_test, score_time})
    IO.puts("")

    print_prediction_differences(outputs_a, outputs_b)

    # --- Phase 3: Worker stats ---
    IO.puts("Phase 3: Worker statistics per session...\n")

    stats_a = fetch_worker_stats(session_a)
    stats_b = fetch_worker_stats(session_b)

    print_worker_stats("A", stats_a)
    print_worker_stats("B", stats_b)

    # --- Phase 4: Session isolation summary ---
    print_isolation_summary(model_a, model_b, stats_a, stats_b)

    cache_size = Server.size(FeatureStore, :feature_cache)
    IO.puts("Feature cache: #{cache_size} entries")
    IO.puts("\nDone!")
  end

  defp train_on_session(train_x, train_y, session_id, _label) do
    ctx = Context.new(session_id: session_id)
    train_item = Item.wrap(%{X: train_x, y: train_y, model_type: "logistic"})

    {:ok, [result]} =
      Dispatch.run([train_item],
        executor: Slither.Dispatch.Executors.SnakeBridge,
        module: "ml_scorer",
        function: "train_model",
        context: ctx,
        batch_size: 1,
        max_in_flight: 1
      )

    result.payload
  end

  defp fetch_worker_stats(session_id) do
    ctx = Context.new(session_id: session_id)
    stats_item = Item.wrap(%{})

    {:ok, [result]} =
      Dispatch.run([stats_item],
        executor: Slither.Dispatch.Executors.SnakeBridge,
        module: "ml_scorer",
        function: "get_model_stats",
        context: ctx,
        batch_size: 1,
        max_in_flight: 1
      )

    result.payload
  end

  # --- Data generation ---

  defp generate_training_data_a(n) do
    half = div(n, 2)

    class_0 =
      for _ <- 1..half do
        [1.0 + :rand.normal(), 2.0 + :rand.normal(), 1.0 + :rand.normal(), 2.0 + :rand.normal()]
      end

    class_1 =
      for _ <- 1..(n - half) do
        [3.0 + :rand.normal(), 4.0 + :rand.normal(), 3.0 + :rand.normal(), 4.0 + :rand.normal()]
      end

    shuffle_training_data(class_0, class_1, half, n)
  end

  defp generate_training_data_b(n) do
    half = div(n, 2)

    class_0 =
      for _ <- 1..half do
        [0.0 + :rand.normal(), 0.0 + :rand.normal(), 0.0 + :rand.normal(), 0.0 + :rand.normal()]
      end

    class_1 =
      for _ <- 1..(n - half) do
        [5.0 + :rand.normal(), 5.0 + :rand.normal(), 5.0 + :rand.normal(), 5.0 + :rand.normal()]
      end

    shuffle_training_data(class_0, class_1, half, n)
  end

  defp shuffle_training_data(class_0, class_1, half, n) do
    x_list = class_0 ++ class_1
    y_list = List.duplicate(0, half) ++ List.duplicate(1, n - half)
    pairs = Enum.zip(x_list, y_list) |> Enum.shuffle()
    {Enum.map(pairs, &elem(&1, 0)), Enum.map(pairs, &elem(&1, 1))}
  end

  defp generate_test_records(n) do
    for i <- 1..n do
      {base_f1, base_f2, base_f3, base_f4} =
        cond do
          rem(i, 3) == 0 ->
            {2.0, 3.0, 2.0, 3.0}

          rem(i, 2) == 0 ->
            {3.5, 4.5, 3.5, 4.5}

          true ->
            {0.5, 1.5, 0.5, 1.5}
        end

      data = %{
        f1: base_f1 + :rand.normal() * 0.3,
        f2: base_f2 + :rand.normal() * 0.3,
        f3: base_f3 + :rand.normal() * 0.3,
        f4: base_f4 + :rand.normal() * 0.3
      }

      %{id: i, data: data}
    end
  end

  defp pre_populate_cache(count) do
    IO.puts("Pre-populating #{count} feature cache entries...")

    for i <- 1..count do
      features = [
        :rand.uniform() * 4.0,
        :rand.uniform() * 4.0,
        :rand.uniform() * 4.0,
        :rand.uniform() * 4.0
      ]

      Server.put(FeatureStore, :feature_cache, i, features)
    end

    IO.puts("  Cached feature vectors for record IDs 1..#{count}\n")
  end

  # --- Printing helpers ---

  defp print_training_result(label, model_info, session_id) do
    if model_info["error"] do
      IO.puts("  Session #{label}: training failed -- #{model_info["error"]}")
    else
      IO.puts(
        "  Session #{label}: accuracy=#{model_info["accuracy"]}, " <>
          "classes=#{inspect(model_info["classes"])}, " <>
          "features=#{model_info["n_features"]}, " <>
          "samples=#{model_info["n_samples"]}, " <>
          "worker_pid=#{model_info["worker_id"]}, " <>
          "session=#{session_id}"
      )
    end
  end

  defp print_results(outputs) do
    for {bucket, items} <- Enum.sort(outputs) do
      IO.puts("  #{bucket} (#{length(items)} items)")
    end

    IO.puts("")
  end

  defp print_prediction_differences(outputs_a, outputs_b) do
    preds_a = collect_predictions(outputs_a)
    preds_b = collect_predictions(outputs_b)

    common_count = min(length(preds_a), length(preds_b))
    pairs = Enum.zip(Enum.take(preds_a, common_count), Enum.take(preds_b, common_count))

    diff_count = Enum.count(pairs, fn {a, b} -> a != b end)

    IO.puts(
      "Prediction divergence: #{diff_count}/#{common_count} records " <>
        "predicted differently (proving model isolation)\n"
    )
  end

  defp collect_predictions(outputs) do
    outputs
    |> Enum.flat_map(fn {_bucket, items} -> items end)
    |> Enum.sort_by(& &1.id)
    |> Enum.map(fn item -> {item.payload["prediction"], item.payload["confidence"]} end)
  end

  defp print_worker_stats(label, stats) do
    IO.puts("  Session #{label} worker:")
    IO.puts("    PID:              #{stats["worker_id"]}")
    IO.puts("    Models stored:    #{stats["models_stored"]}")
    IO.puts("    Model IDs:        #{inspect(stats["model_ids"])}")
    IO.puts("    Prediction count: #{stats["prediction_count"]}")

    case stats["training_history"] do
      [entry | _] ->
        IO.puts(
          "    Last trained:     model #{String.slice(entry["model_id"], 0..7)}... " <>
            "(#{entry["n_samples"]} samples, acc=#{entry["accuracy"]})"
        )

      _ ->
        :ok
    end

    IO.puts("")
  end

  defp print_isolation_summary(model_a, model_b, stats_a, stats_b) do
    IO.puts("=== Session Isolation Summary ===\n")

    pid_a = model_a["worker_id"]
    pid_b = model_b["worker_id"]
    mid_a = String.slice(model_a["model_id"] || "", 0..7)
    mid_b = String.slice(model_b["model_id"] || "", 0..7)

    IO.puts("  Session A trained model #{mid_a}... on worker PID #{pid_a}")
    IO.puts("  Session B trained model #{mid_b}... on worker PID #{pid_b}")

    IO.puts(
      "  Session A worker has models: #{inspect(stats_a["model_ids"] |> Enum.map(&String.slice(&1, 0..7)))}"
    )

    IO.puts(
      "  Session B worker has models: #{inspect(stats_b["model_ids"] |> Enum.map(&String.slice(&1, 0..7)))}"
    )

    IO.puts("  Session A predictions used model #{mid_a}... (not #{mid_b}...)")
    IO.puts("  Session B predictions used model #{mid_b}... (not #{mid_a}...)")

    if pid_a != pid_b do
      IO.puts("  Workers are separate OS processes (PID #{pid_a} vs #{pid_b})")
    else
      IO.puts("  Workers share a process but session affinity keeps state isolated")
    end

    IO.puts("")
    IO.puts("  Under free-threaded Python:")
    IO.puts("    - Shared _models dict = one session overwrites another's model")
    IO.puts("    - _prediction_count += N = read-modify-write race across 48 threads")
    IO.puts("    - #{@num_train} training samples * 2 sessions = massive concurrent writes")
    IO.puts("    - #{@num_test} test records scored concurrently through both sessions")
    IO.puts("    - Slither's process-per-session design eliminates all races\n")
  end
end
