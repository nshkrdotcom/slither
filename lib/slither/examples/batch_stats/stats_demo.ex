defmodule Slither.Examples.BatchStats.StatsDemo do
  @moduledoc """
  Demonstrates Dispatch with all three batching strategies, streaming, and
  fault isolation via per-worker process boundaries.

  Generates 50 numeric datasets of varying sizes (small, medium, large) plus
  5 "poison pill" datasets that contain invalid data (nil, NaN, empty).
  Dispatches them to a Python `batch_stats` module using:

    1. **FixedBatch** -- fixed chunks of 10 items
    2. **WeightedBatch** -- batches capped at 500 total values
    3. **KeyPartition** -- one batch per dataset size category
    4. **Streaming** -- lazy stream with `as_completed` ordering

  After all strategy runs, queries each worker for its accumulated running
  statistics (Welford's online mean/variance) to show that per-process state
  remains internally consistent -- something impossible under free-threaded
  Python where concurrent threads corrupt shared accumulators.

  Run with `Slither.Examples.BatchStats.StatsDemo.run_demo/0`.
  """

  alias Slither.Dispatch
  alias Slither.Dispatch.Strategies.FixedBatch
  alias Slither.Dispatch.Strategies.KeyPartition
  alias Slither.Dispatch.Strategies.WeightedBatch
  alias Slither.Item

  @doc """
  Run the full batch statistics demo, printing results to stdout.
  """
  def run_demo do
    IO.puts("\n=== Slither Example: Batch Statistics ===\n")

    datasets = generate_datasets()
    items = Item.wrap_many(datasets)

    fixed_results = run_fixed_batch(items)
    weighted_results = run_weighted_batch(items)
    partition_results = run_key_partition(items)
    stream_count = run_streaming(items)

    print_fault_isolation_summary(
      fixed_results,
      weighted_results,
      partition_results,
      stream_count
    )

    run_worker_stats()

    IO.puts("Done!")
  end

  # ---------------------------------------------------------------------------
  # Dataset generation
  # ---------------------------------------------------------------------------

  defp generate_datasets do
    # Seed the random number generator for reproducible demo output
    :rand.seed(:exsss, {42, 137, 256})

    small = for _ <- 1..20, do: make_dataset(:small, 5, 10)
    medium = for _ <- 1..20, do: make_dataset(:medium, 50, 100)
    large = for _ <- 1..10, do: make_dataset(:large, 200, 500)

    poison = generate_poison_pills()

    # Interleave so batching strategies see a realistic mix
    (small ++ medium ++ large ++ poison)
    |> Enum.shuffle()
  end

  defp make_dataset(data_type, min_size, max_size) do
    size = min_size + :rand.uniform(max_size - min_size + 1) - 1
    values = for _ <- 1..size, do: :rand.uniform(1000) / 1.0

    %{values: values, data_type: data_type}
  end

  defp generate_poison_pills do
    # 2 datasets with nil values mixed in
    nil_datasets =
      for _ <- 1..2 do
        values = [1.0, 2.0, nil, 4.0, nil, 6.0]
        %{values: values, data_type: :poison}
      end

    # 2 datasets with NaN values
    nan_datasets =
      for _ <- 1..2 do
        values = [10.0, "NaN", 30.0, "NaN", 50.0]
        %{values: values, data_type: :poison}
      end

    # 1 empty dataset
    empty_dataset = [%{values: [], data_type: :poison}]

    nil_datasets ++ nan_datasets ++ empty_dataset
  end

  # ---------------------------------------------------------------------------
  # FixedBatch strategy
  # ---------------------------------------------------------------------------

  defp run_fixed_batch(items) do
    IO.puts("--- FixedBatch (batch_size: 10) ---")

    {time, {:ok, results}} =
      :timer.tc(fn ->
        Dispatch.run(items,
          executor: Slither.Dispatch.Executors.SnakeBridge,
          module: "batch_stats",
          function: "compute_stats",
          strategy: FixedBatch,
          batch_size: 10,
          max_in_flight: 4,
          on_error: :skip
        )
      end)

    {succeeded, failed} = count_results(results)
    IO.puts("  #{succeeded} succeeded, #{failed} failed/skipped in #{div(time, 1000)}ms")
    print_sample(results)

    results
  end

  # ---------------------------------------------------------------------------
  # WeightedBatch strategy
  # ---------------------------------------------------------------------------

  defp run_weighted_batch(items) do
    IO.puts("--- WeightedBatch (max_weight: 500) ---")

    {time, {:ok, results}} =
      :timer.tc(fn ->
        Dispatch.run(items,
          executor: Slither.Dispatch.Executors.SnakeBridge,
          module: "batch_stats",
          function: "compute_stats",
          strategy: WeightedBatch,
          max_weight: 500,
          weight_fn: fn item -> length(item.payload[:values]) end,
          max_in_flight: 4,
          on_error: :skip
        )
      end)

    {succeeded, failed} = count_results(results)
    IO.puts("  #{succeeded} succeeded, #{failed} failed/skipped in #{div(time, 1000)}ms")
    print_sample(results)

    results
  end

  # ---------------------------------------------------------------------------
  # KeyPartition strategy
  # ---------------------------------------------------------------------------

  defp run_key_partition(items) do
    IO.puts("--- KeyPartition (by data_type) ---")

    {time, {:ok, results}} =
      :timer.tc(fn ->
        Dispatch.run(items,
          executor: Slither.Dispatch.Executors.SnakeBridge,
          module: "batch_stats",
          function: "compute_stats",
          strategy: KeyPartition,
          key_fn: fn item -> item.payload[:data_type] end,
          max_in_flight: 4,
          on_error: :skip
        )
      end)

    {succeeded, failed} = count_results(results)
    IO.puts("  #{succeeded} succeeded, #{failed} failed/skipped in #{div(time, 1000)}ms")
    print_partitioned(results)

    results
  end

  # ---------------------------------------------------------------------------
  # Streaming
  # ---------------------------------------------------------------------------

  defp run_streaming(items) do
    IO.puts("--- Streaming (as_completed) ---")

    {time, results} =
      :timer.tc(fn ->
        Dispatch.stream(items,
          executor: Slither.Dispatch.Executors.SnakeBridge,
          module: "batch_stats",
          function: "compute_stats",
          strategy: FixedBatch,
          batch_size: 10,
          max_in_flight: 2,
          ordering: :as_completed,
          on_error: :skip
        )
        |> Enum.to_list()
      end)

    {succeeded, failed} = count_results(results)

    IO.puts(
      "  Streamed #{succeeded} succeeded, #{failed} failed/skipped in #{div(time, 1000)}ms\n"
    )

    length(results)
  end

  # ---------------------------------------------------------------------------
  # Worker running stats
  # ---------------------------------------------------------------------------

  defp run_worker_stats do
    IO.puts("--- Worker Running Stats ---")
    IO.puts("  Querying per-worker accumulated Welford statistics...\n")

    # Send a single-item batch to each worker to retrieve its running stats.
    # We send multiple items so the pool fans out across workers.
    probe_items = Item.wrap_many(for _ <- 1..4, do: %{probe: true})

    {:ok, results} =
      Dispatch.run(probe_items,
        executor: Slither.Dispatch.Executors.SnakeBridge,
        module: "batch_stats",
        function: "get_running_stats",
        strategy: FixedBatch,
        batch_size: 1,
        max_in_flight: 4,
        on_error: :skip
      )

    print_worker_stats(results)
  end

  defp print_worker_stats(results) do
    results
    |> Enum.reject(fn item -> item.meta[:skipped] end)
    |> Enum.uniq_by(fn item -> item.payload["worker_id"] end)
    |> Enum.each(fn item ->
      p = item.payload
      IO.puts("  Worker PID #{p["worker_id"]}:")
      IO.puts("    batches_processed: #{p["batches_processed"]}")
      IO.puts("    running_n:         #{p["running_n"]}")
      IO.puts("    running_mean:      #{p["running_mean"]}")
      IO.puts("    running_variance:  #{p["running_variance"]}")
      IO.puts("    running_min:       #{p["running_min"]}")
      IO.puts("    running_max:       #{p["running_max"]}")
    end)

    IO.puts("")
    IO.puts("  Each worker's Welford accumulator is internally consistent because")
    IO.puts("  Slither runs workers in separate OS processes -- updates are sequential.")
    IO.puts("  Under free-threaded Python, concurrent threads would interleave the")
    IO.puts("  multi-step mean/variance update, producing incorrect statistics.\n")
  end

  # ---------------------------------------------------------------------------
  # Fault isolation summary
  # ---------------------------------------------------------------------------

  defp print_fault_isolation_summary(fixed, weighted, partition, stream_count) do
    IO.puts("=== Fault Isolation Summary ===\n")

    fixed_failed = count_failed(fixed)
    weighted_failed = count_failed(weighted)
    partition_failed = count_failed(partition)

    total_poison = 5
    total_failed = fixed_failed + weighted_failed + partition_failed

    total_succeeded =
      length(fixed) - fixed_failed + length(weighted) - weighted_failed +
        length(partition) - partition_failed + stream_count

    IO.puts("  #{total_poison} poison-pill datasets were injected (nil, NaN, empty)")
    IO.puts("  #{total_failed} batch items failed/skipped across all strategies")
    IO.puts("  #{total_succeeded} results still completed successfully")
    IO.puts("  Under free-threaded Python, one bad dataset crashes all threads.\n")
  end

  # ---------------------------------------------------------------------------
  # Output helpers
  # ---------------------------------------------------------------------------

  defp count_results(results) do
    {succeeded, failed} =
      Enum.split_with(results, fn item ->
        not Map.get(item.meta, :skipped, false)
      end)

    {length(succeeded), length(failed)}
  end

  defp count_failed(results) do
    Enum.count(results, fn item -> Map.get(item.meta, :skipped, false) end)
  end

  defp print_sample(results) do
    results
    |> Enum.reject(fn item -> item.meta[:skipped] end)
    |> Enum.take(3)
    |> Enum.each(fn item ->
      p = item.payload
      IO.puts("  mean=#{p["mean"]} stdev=#{p["stdev"]} range=#{p["range"]} count=#{p["count"]}")
    end)

    IO.puts("")
  end

  defp print_partitioned(results) do
    {succeeded, _failed} = count_results(results)
    IO.puts("  #{succeeded} total results across partitions")

    results
    |> Enum.reject(fn item -> item.meta[:skipped] end)
    |> Enum.take(2)
    |> Enum.each(fn item ->
      p = item.payload
      IO.puts("  mean=#{p["mean"]} stdev=#{p["stdev"]} count=#{p["count"]}")
    end)

    IO.puts("")
  end
end
