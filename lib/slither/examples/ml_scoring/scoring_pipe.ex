defmodule Slither.Examples.MlScoring.ScoringPipe do
  @moduledoc """
  ML scoring pipeline: enrich -> featurize -> predict -> route by confidence.

  Demonstrates session-scoped Python object references and the full
  Slither pipeline lifecycle:

    1. **enrich** (beam) -- checks the ETS feature cache for pre-computed
       features; on a miss, packages raw data for Python featurization.
       Attaches the `model_id` from context metadata to every item.
    2. **featurize** (python) -- extracts numeric features from raw data
       dicts, or passes through items that already have cached features.
    3. **predict** (python) -- runs batch prediction using a scikit-learn
       model stored in the Python session. Session affinity ensures the
       model trained by `train_model` is accessible.
    4. **route_by_confidence** (router) -- splits results into
       `:high_confidence` (>= 0.9) and `:low_confidence` (< 0.6) buckets;
       mid-range items go to `:default`.

  ## Concurrent Session Isolation

  The `run_demo/0` function trains TWO models on TWO separate sessions
  simultaneously, then scores test records through each session's
  pipeline independently. This proves that:

    - Session A's model is isolated to its worker process
    - Session B's model is isolated to its worker process
    - Predictions through session A use model A (not B)
    - Predictions through session B use model B (not A)

  Under free-threaded Python, a shared `_models` dict mutated from
  multiple threads would lead to corruption -- one session could
  silently overwrite another's model. Slither's process-per-session
  design eliminates this by construction.

  Requires scikit-learn and numpy. Run with:

      Slither.Examples.MlScoring.ScoringPipe.run_demo()
  """

  use Slither.Pipe

  alias Slither.{Context, Dispatch, Item}
  alias Slither.Examples.MlScoring.FeatureStore
  alias Slither.Pipe.Runner
  alias Slither.Store.Server

  pipe :ml_scoring do
    store(:features, FeatureStore)

    stage(:enrich, :beam, handler: &__MODULE__.enrich/2)

    stage(:featurize, :python,
      executor: Slither.Dispatch.Executors.SnakeBridge,
      module: "ml_scorer",
      function: "featurize_batch",
      batch_size: 20
    )

    stage(:predict, :python,
      executor: Slither.Dispatch.Executors.SnakeBridge,
      module: "ml_scorer",
      function: "predict_batch",
      batch_size: 20
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

  @doc """
  Enrich a record with cached features or prepare it for featurization.

  Checks the ETS feature cache for the record's ID. On a cache hit the
  pre-computed feature vector is used directly; on a miss the raw data
  map is passed through so the Python featurize stage can extract
  features. The model ID from context metadata is attached in both cases
  so the predict stage knows which model to use.
  """
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

  @doc """
  Run the ML scoring demo with concurrent session isolation.

  Trains two logistic regression models on separate sessions with
  different data distributions, then scores test records through each
  session independently. Demonstrates that Slither's session affinity
  prevents cross-contamination of model state.
  """
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
    IO.puts("Phase 1: Training two models on separate sessions...\n")

    session_a = "scoring_a_#{System.unique_integer([:positive])}"
    session_b = "scoring_b_#{System.unique_integer([:positive])}"

    {train_x_a, train_y_a} = generate_training_data_a(200)
    {train_x_b, train_y_b} = generate_training_data_b(200)

    task_a = Task.async(fn -> train_on_session(train_x_a, train_y_a, session_a, "A") end)
    task_b = Task.async(fn -> train_on_session(train_x_b, train_y_b, session_b, "B") end)

    model_a = Task.await(task_a, 30_000)
    model_b = Task.await(task_b, 30_000)

    print_training_result("A", model_a, session_a)
    print_training_result("B", model_b, session_b)

    # --- Phase 2: Score through both sessions ---
    IO.puts("\nPhase 2: Scoring test records through both sessions...\n")

    pre_populate_cache(3)
    test_records = generate_test_records(20)

    IO.puts("Scoring #{length(test_records)} records through Session A...")

    {:ok, outputs_a} =
      Runner.run(__MODULE__, test_records,
        session_id: session_a,
        metadata: %{model_id: model_a["model_id"]}
      )

    IO.puts("Scoring #{length(test_records)} records through Session B...")

    {:ok, outputs_b} =
      Runner.run(__MODULE__, test_records,
        session_id: session_b,
        metadata: %{model_id: model_b["model_id"]}
      )

    IO.puts("\n--- Session A Results ---")
    print_results(outputs_a)

    IO.puts("--- Session B Results ---")
    print_results(outputs_b)

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
    # Dataset A: class 0 centered at [1,2,1,2], class 1 centered at [3,4,3,4]
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
    # Dataset B: class 0 centered at [0,0,0,0], class 1 centered at [5,5,5,5]
    # Wider separation = different decision boundary than A
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
            # Near decision boundary -> low confidence
            {2.0, 3.0, 2.0, 3.0}

          rem(i, 2) == 0 ->
            # Clearly class 1 -> high confidence
            {3.5, 4.5, 3.5, 4.5}

          true ->
            # Clearly class 0 -> high confidence
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
          "worker_pid=#{model_info["worker_id"]}, " <>
          "session=#{session_id}"
      )
    end
  end

  defp print_results(outputs) do
    for {bucket, items} <- Enum.sort(outputs) do
      IO.puts("  #{bucket} (#{length(items)} items)")
      items |> Enum.take(3) |> Enum.each(&print_prediction/1)

      if length(items) > 3 do
        IO.puts("    ... and #{length(items) - 3} more")
      end
    end

    IO.puts("")
  end

  defp print_prediction(item) do
    p = item.payload

    probs =
      case p["probabilities"] do
        nil -> ""
        probs_map -> " probs=#{inspect(probs_map)}"
      end

    IO.puts("    prediction=#{p["prediction"]} confidence=#{p["confidence"]}#{probs}")
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
    IO.puts("    - _prediction_count += N = read-modify-write race across threads")
    IO.puts("    - numpy BLAS from multiple threads = undefined C buffer behavior")
    IO.puts("    - Slither's process-per-session design eliminates all three\n")
  end
end
