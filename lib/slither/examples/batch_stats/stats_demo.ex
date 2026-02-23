defmodule Slither.Examples.BatchStats.StatsDemo do
  @moduledoc """
  Demonstrates Dispatch with all three batching strategies, streaming, and
  fault isolation via per-worker process boundaries.

  Generates 2,000 numeric datasets (800 small, 800 medium, 350 large, 50 poison)
  and dispatches them through 48 workers using FixedBatch, WeightedBatch,
  KeyPartition, and streaming modes.

  After all strategy runs, queries each worker for accumulated Welford statistics
  to prove per-process consistency.

  Run with `Slither.Examples.BatchStats.StatsDemo.run_demo/0`.
  """

  alias Slither.Dispatch
  alias Slither.Dispatch.Strategies.FixedBatch
  alias Slither.Dispatch.Strategies.KeyPartition
  alias Slither.Dispatch.Strategies.WeightedBatch
  alias Slither.Examples.Reporter
  alias Slither.Item

  def run_demo do
    IO.puts("\n=== Slither Example: Batch Statistics ===\n")

    datasets = generate_datasets()
    items = Item.wrap_many(datasets)
    IO.puts("Generated #{length(datasets)} datasets (#{count_by_type(datasets)})\n")

    {t1, fixed_results} = :timer.tc(fn -> run_fixed_batch(items) end)
    {t2, weighted_results} = :timer.tc(fn -> run_weighted_batch(items) end)
    {t3, partition_results} = :timer.tc(fn -> run_key_partition(items) end)
    {t4, stream_count} = :timer.tc(fn -> run_streaming(items) end)

    IO.puts("--- Timing ---")
    Reporter.print_timing("FixedBatch", t1)
    Reporter.print_timing("WeightedBatch", t2)
    Reporter.print_timing("KeyPartition", t3)
    Reporter.print_timing("Streaming", t4)
    IO.puts("")

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
  # Dataset generation — 2,000 total
  # ---------------------------------------------------------------------------

  defp generate_datasets do
    :rand.seed(:exsss, {42, 137, 256})

    small = for _ <- 1..800, do: make_dataset(:small, 5, 10)
    medium = for _ <- 1..800, do: make_dataset(:medium, 50, 100)
    large = for _ <- 1..350, do: make_dataset(:large, 200, 500)
    poison = generate_poison_pills()

    (small ++ medium ++ large ++ poison)
    |> Enum.shuffle()
  end

  defp make_dataset(data_type, min_size, max_size) do
    size = min_size + :rand.uniform(max_size - min_size + 1) - 1
    values = for _ <- 1..size, do: :rand.uniform(1000) / 1.0

    %{values: values, data_type: data_type}
  end

  defp generate_poison_pills do
    nil_datasets =
      for _ <- 1..20 do
        values = [1.0, 2.0, nil, 4.0, nil, 6.0]
        %{values: values, data_type: :poison}
      end

    nan_datasets =
      for _ <- 1..20 do
        values = [10.0, "NaN", 30.0, "NaN", 50.0]
        %{values: values, data_type: :poison}
      end

    empty_datasets = for _ <- 1..10, do: %{values: [], data_type: :poison}

    nil_datasets ++ nan_datasets ++ empty_datasets
  end

  defp count_by_type(datasets) do
    counts = Enum.frequencies_by(datasets, & &1.data_type)

    [:small, :medium, :large, :poison]
    |> Enum.map_join(", ", fn type -> "#{Map.get(counts, type, 0)} #{type}" end)
  end

  # ---------------------------------------------------------------------------
  # FixedBatch strategy
  # ---------------------------------------------------------------------------

  defp run_fixed_batch(items) do
    IO.puts("--- FixedBatch (batch_size: 10) ---")

    {:ok, results} =
      Dispatch.run(items,
        executor: Slither.Dispatch.Executors.SnakeBridge,
        module: "batch_stats",
        function: "compute_stats",
        strategy: FixedBatch,
        batch_size: 10,
        max_in_flight: 16,
        on_error: :skip
      )

    {succeeded, failed} = count_results(results)
    IO.puts("  #{succeeded} succeeded, #{failed} failed/skipped")
    print_sample(results)

    results
  end

  # ---------------------------------------------------------------------------
  # WeightedBatch strategy
  # ---------------------------------------------------------------------------

  defp run_weighted_batch(items) do
    IO.puts("--- WeightedBatch (max_weight: 500) ---")

    {:ok, results} =
      Dispatch.run(items,
        executor: Slither.Dispatch.Executors.SnakeBridge,
        module: "batch_stats",
        function: "compute_stats",
        strategy: WeightedBatch,
        max_weight: 500,
        weight_fn: fn item -> length(item.payload[:values]) end,
        max_in_flight: 16,
        on_error: :skip
      )

    {succeeded, failed} = count_results(results)
    IO.puts("  #{succeeded} succeeded, #{failed} failed/skipped")
    print_sample(results)

    results
  end

  # ---------------------------------------------------------------------------
  # KeyPartition strategy
  # ---------------------------------------------------------------------------

  defp run_key_partition(items) do
    IO.puts("--- KeyPartition (by data_type) ---")

    {:ok, results} =
      Dispatch.run(items,
        executor: Slither.Dispatch.Executors.SnakeBridge,
        module: "batch_stats",
        function: "compute_stats",
        strategy: KeyPartition,
        key_fn: fn item -> item.payload[:data_type] end,
        max_in_flight: 16,
        on_error: :skip
      )

    {succeeded, failed} = count_results(results)
    IO.puts("  #{succeeded} succeeded, #{failed} failed/skipped")
    print_partitioned(results)

    results
  end

  # ---------------------------------------------------------------------------
  # Streaming
  # ---------------------------------------------------------------------------

  defp run_streaming(items) do
    IO.puts("--- Streaming (as_completed) ---")

    results =
      Dispatch.stream(items,
        executor: Slither.Dispatch.Executors.SnakeBridge,
        module: "batch_stats",
        function: "compute_stats",
        strategy: FixedBatch,
        batch_size: 10,
        max_in_flight: 8,
        ordering: :as_completed,
        on_error: :skip
      )
      |> Enum.to_list()

    {succeeded, failed} = count_results(results)
    IO.puts("  Streamed #{succeeded} succeeded, #{failed} failed/skipped\n")

    length(results)
  end

  # ---------------------------------------------------------------------------
  # Worker running stats
  # ---------------------------------------------------------------------------

  defp run_worker_stats do
    IO.puts("--- Worker Running Stats ---")
    IO.puts("  Querying per-worker accumulated Welford statistics...\n")

    probe_items = Item.wrap_many(for _ <- 1..200, do: %{probe: true})

    {:ok, results} =
      Dispatch.run(probe_items,
        executor: Slither.Dispatch.Executors.SnakeBridge,
        module: "batch_stats",
        function: "get_running_stats",
        strategy: FixedBatch,
        batch_size: 1,
        max_in_flight: 16,
        on_error: :skip
      )

    print_worker_stats(results)
  end

  defp print_worker_stats(results) do
    unique =
      results
      |> Enum.reject(fn item ->
        item.meta[:skipped] == true or item.payload["worker_id"] == nil
      end)
      |> Enum.uniq_by(fn item -> item.payload["worker_id"] end)

    for stats <- Enum.take(unique, 10) do
      p = stats.payload
      IO.puts("  Worker PID #{p["worker_id"]}:")
      IO.puts("    batches_processed: #{p["batches_processed"]}")
      IO.puts("    running_n:         #{p["running_n"]}")
      IO.puts("    running_mean:      #{p["running_mean"]}")
      IO.puts("    running_variance:  #{p["running_variance"]}")
    end

    if length(unique) > 10 do
      IO.puts("  ... and #{length(unique) - 10} more workers")
    end

    IO.puts("\n  Workers reached: #{length(unique)} of 48")
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

    total_poison = 50
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
