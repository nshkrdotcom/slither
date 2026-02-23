defmodule Slither.Examples.TextAnalysis.TextPipe do
  @moduledoc """
  Text analysis pipeline demonstrating why process isolation beats free-threaded Python.

  Processes 5,000 documents through 48 workers with:
    - `batch_size: 50` to amortize gRPC overhead
    - `max_in_flight: 16` for concurrent batch processing

  **Phase 1 -- Pipe run:** prepare -> analyze -> classify by sentiment.
  **Phase 2 -- Worker stats collection:** Probes all 48 workers to prove
  per-process isolation of `_global_index` and `_doc_count`.

  Run with `Slither.Examples.TextAnalysis.TextPipe.run_demo/0`.
  """

  use Slither.Pipe

  alias Slither.Dispatch
  alias Slither.Examples.Reporter
  alias Slither.Examples.TextAnalysis.StopwordStore
  alias Slither.Item
  alias Slither.Pipe.Runner
  alias Slither.Store.Server

  @num_docs 5000

  @vocab ~w(
    good bad excellent terrible service quality today result happy sad
    amazing awful fast slow reliable broken love hate recommend avoid
    product team experience delivery price support feedback performance
    design usability innovation problem solution customer value improve
  )

  pipe :text_analysis do
    store(:stopwords, StopwordStore)

    stage(:prepare, :beam, handler: &__MODULE__.prepare/2)

    stage(:analyze, :python,
      executor: Slither.Dispatch.Executors.SnakeBridge,
      module: "text_analyzer",
      function: "analyze_batch",
      batch_size: 50
    )

    stage(:classify, :router,
      routes: [
        {fn item -> item.payload["sentiment"] > 0.15 end, :positive},
        {fn item -> item.payload["sentiment"] < -0.15 end, :negative}
      ]
    )

    output(:positive)
    output(:negative)

    on_error(:analyze, :skip)
    on_error(:*, :halt)
  end

  def prepare(item, _ctx) do
    stopwords =
      Server.fold(StopwordStore, :stopwords, [], fn {word, _}, acc ->
        [word | acc]
      end)

    %{"text" => item.payload, "stopwords" => stopwords}
  end

  def run_demo do
    {:ok, _pid} = Server.start_link(module: StopwordStore, name: StopwordStore)
    Process.sleep(100)

    texts = build_corpus()

    IO.puts("\n=== Slither Example: Text Analysis Pipeline ===")
    IO.puts("=== Why Process Isolation Beats Free-Threaded Python ===\n")
    IO.puts("Processing #{length(texts)} texts through: prepare -> analyze -> classify\n")

    {time, {:ok, outputs}} = :timer.tc(fn -> Runner.run(__MODULE__, texts) end)

    print_pipeline_results(outputs)
    Reporter.print_timing("Pipeline elapsed", time)
    Reporter.print_throughput("Throughput", {length(texts), time})
    IO.puts("")

    collect_and_print_worker_stats(length(texts))
    print_why_it_matters()

    IO.puts("Done!")
  end

  # ---------------------------------------------------------------------------
  # Corpus: 5,000 documents via seeded random generation
  # ---------------------------------------------------------------------------

  defp build_corpus do
    :rand.seed(:exsss, {42, 137, 256})
    vocab = @vocab

    for _ <- 1..@num_docs do
      word_count = 15 + :rand.uniform(50)

      words =
        for _ <- 1..word_count do
          Enum.at(vocab, :rand.uniform(length(vocab)) - 1)
        end

      Enum.join(words, " ")
    end
  end

  # ---------------------------------------------------------------------------
  # Phase 1: Pipeline results
  # ---------------------------------------------------------------------------

  defp print_pipeline_results(outputs) do
    IO.puts("--- Pipeline Results ---\n")

    total = Enum.reduce(outputs, 0, fn {_bucket, items}, acc -> acc + length(items) end)

    for {bucket, items} <- Enum.sort(outputs) do
      IO.puts("  #{bucket}: #{length(items)} items")
    end

    IO.puts("  total classified: #{total}")
    IO.puts("")
  end

  # ---------------------------------------------------------------------------
  # Phase 2: Worker stats collection and merge
  # ---------------------------------------------------------------------------

  defp collect_and_print_worker_stats(expected_docs) do
    IO.puts("--- Worker Isolation Proof ---\n")

    # Send 200 probe items at batch_size 1 to maximize worker coverage
    probe_items = Item.wrap_many(for _ <- 1..200, do: "stats_probe")

    case fetch_worker_stats(probe_items) do
      {:ok, stats_results} ->
        worker_stats =
          stats_results
          |> Enum.map(& &1.payload)
          |> Enum.reject(fn s -> s["worker_id"] == nil or s["worker_id"] == "" end)
          |> Enum.uniq_by(& &1["worker_id"])

        print_per_worker_stats(worker_stats)
        merged = merge_worker_stats(worker_stats)
        print_merged_stats(merged)

        IO.puts("  Workers reached: #{length(worker_stats)} of 48\n")
        Reporter.print_correctness("Doc count check", expected_docs, merged.total_docs)
        IO.puts("")

      {:error, reason, _partial} ->
        IO.puts("  [warning] Could not collect worker stats: #{inspect(reason)}\n")
    end
  end

  defp fetch_worker_stats(probe_items) do
    Dispatch.run(probe_items,
      executor: Slither.Dispatch.Executors.SnakeBridge,
      module: "text_analyzer",
      function: "get_worker_stats",
      batch_size: 1,
      max_in_flight: 16,
      on_error: :skip
    )
  end

  defp print_per_worker_stats(worker_stats) do
    IO.puts("  Per-worker accumulated state (each is a separate OS process):\n")

    for stats <- Enum.take(worker_stats, 10) do
      pid = stats["worker_id"]
      docs = stats["doc_count"]
      vocab = stats["vocab_size"]
      total = stats["total_word_occurrences"]

      IO.puts("    Worker PID #{pid}: #{docs} docs, #{vocab} unique words, #{total} occurrences")
    end

    if length(worker_stats) > 10 do
      IO.puts("    ... and #{length(worker_stats) - 10} more workers")
    end

    IO.puts("")
  end

  defp merge_worker_stats(worker_stats) do
    total_docs = Enum.sum(Enum.map(worker_stats, &(&1["doc_count"] || 0)))

    all_words =
      worker_stats
      |> Enum.flat_map(fn stats -> Map.to_list(stats["top_words"] || %{}) end)
      |> Enum.reduce(%{}, fn {word, count}, acc ->
        Map.update(acc, word, count, &(&1 + count))
      end)

    top_20 =
      all_words
      |> Enum.sort_by(fn {_word, count} -> -count end)
      |> Enum.take(20)

    total_vocab = map_size(all_words)

    %{
      total_docs: total_docs,
      total_vocab: total_vocab,
      top_20: top_20
    }
  end

  defp print_merged_stats(merged) do
    IO.puts("  Merged view (computed in a single Elixir process -- race-free):\n")
    IO.puts("    Total documents processed: #{merged.total_docs}")
    IO.puts("    Combined vocabulary size:  #{merged.total_vocab}")
    IO.puts("    Top 20 words across all workers:")

    for {word, count} <- merged.top_20 do
      IO.puts("      #{String.pad_trailing(word, 20)} #{count}")
    end

    IO.puts("")
  end

  # ---------------------------------------------------------------------------
  # Explanation
  # ---------------------------------------------------------------------------

  defp print_why_it_matters do
    IO.puts("--- Why This Matters ---\n")

    IO.puts("""
      Under free-threaded Python (PEP 703, Python 3.13+):
        - The GIL is removed, so threads truly run in parallel
        - _global_index[word] += count from 48 threads = lost updates
        - _doc_count += 1 from 48 threads = torn counter
        - 5,000 docs * 48 threads = massive contention window

      Under Slither:
        - Each of the 48 Python workers is a separate OS process
        - _global_index and _doc_count are process-local -- zero contention
        - The Elixir side merges per-worker results in a single process
        - No locks, no mutexes, no atomics -- just process isolation
    """)
  end
end
