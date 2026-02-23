defmodule Slither.Examples.TextAnalysis.TextPipe do
  @moduledoc """
  Text analysis pipeline demonstrating why process isolation beats free-threaded Python.

  The pipeline has two phases:

  **Phase 1 -- Pipe run:** prepare -> analyze -> classify by sentiment.
    1. Prepares raw text by attaching stopwords from an ETS-backed store (beam stage)
    2. Sends batches to Python for word frequency, readability, and sentiment analysis
    3. Routes results into positive, negative, or neutral buckets based on sentiment score

  **Phase 2 -- Worker stats collection:** After the pipe run, uses Dispatch directly to
  call `get_worker_stats` on each Python worker. Each worker reports its accumulated
  state (`_global_index`, `_doc_count`), proving that:
    - Different workers accumulated different subsets of documents
    - Each worker's `_doc_count` is consistent (no torn counters)
    - The Elixir side can merge per-worker results safely in a single process

  Run with `Slither.Examples.TextAnalysis.TextPipe.run_demo/0`.
  """

  use Slither.Pipe

  alias Slither.Dispatch
  alias Slither.Examples.TextAnalysis.StopwordStore
  alias Slither.Item
  alias Slither.Pipe.Runner
  alias Slither.Store.Server

  pipe :text_analysis do
    store(:stopwords, StopwordStore)

    stage(:prepare, :beam, handler: &__MODULE__.prepare/2)

    stage(:analyze, :python,
      executor: Slither.Dispatch.Executors.SnakeBridge,
      module: "text_analyzer",
      function: "analyze_batch",
      batch_size: 10
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

  @doc """
  Prepare a raw text item for Python analysis.

  Reads the full stopword list from the ETS store and packages it
  alongside the text into the map expected by `text_analyzer.analyze_batch`.
  """
  def prepare(item, _ctx) do
    stopwords =
      Server.fold(StopwordStore, :stopwords, [], fn {word, _}, acc ->
        [word | acc]
      end)

    %{"text" => item.payload, "stopwords" => stopwords}
  end

  @doc """
  Run the text analysis demo, printing formatted results to stdout.

  Executes two phases:
  1. The text analysis pipe (prepare -> analyze -> classify)
  2. Worker stats collection via Dispatch (proves process isolation)
  """
  def run_demo do
    {:ok, _pid} = Server.start_link(module: StopwordStore, name: StopwordStore)
    Process.sleep(100)

    texts = build_corpus()

    IO.puts("\n=== Slither Example: Text Analysis Pipeline ===")
    IO.puts("=== Why Process Isolation Beats Free-Threaded Python ===\n")
    IO.puts("Processing #{length(texts)} texts through: prepare -> analyze -> classify\n")

    {:ok, outputs} = Runner.run(__MODULE__, texts)

    print_pipeline_results(outputs)
    collect_and_print_worker_stats()
    print_why_it_matters()

    IO.puts("Done!")
  end

  # ---------------------------------------------------------------------------
  # Corpus: 42 documents mixing positive, negative, and neutral text
  # ---------------------------------------------------------------------------

  defp build_corpus do
    positive_texts() ++ negative_texts() ++ neutral_texts()
  end

  defp positive_texts do
    [
      "The sunrise was absolutely beautiful and magnificent today. I feel wonderful and grateful for this amazing experience.",
      "What a fantastic concert! The musicians were brilliant and the atmosphere was truly delightful and cheerful.",
      "I am so happy with this purchase. The quality is excellent and the customer service was outstanding and kind.",
      "This park is lovely and the gardens are spectacular. A perfect place to spend a pleasant afternoon.",
      "The new restaurant downtown is fabulous. Every dish was superb and the dessert was simply incredible.",
      "Our vacation was thrilling and enjoyable from start to finish. The hotel staff were generous and warm.",
      "The children put on a remarkable performance at the school play. Their talent is truly magnificent.",
      "Working with this team has been a blissful experience. Everyone is supportive and the results are terrific.",
      "The autumn leaves are vibrant and elegant this year. Nature creates the most splendid landscapes.",
      "I received the most charming gift today. It was a wonderful surprise that made my day perfect.",
      "The community garden project has been an outstanding success. Volunteers are happy and the harvest is great.",
      "This book is an amazing journey through history. The writing is brilliant and the research is excellent.",
      "The wedding ceremony was beautiful and the reception was fantastic. Everything was organized perfectly.",
      "Learning to paint has been a delightful experience. My instructor is kind and the classes are enjoyable."
    ]
  end

  defp negative_texts do
    [
      "This restaurant serves terrible food. The service was awful and the prices are outrageous. A truly dreadful experience.",
      "The movie was boring and dull with a stupid plot. What a disappointing waste of time and money.",
      "I had a horrible day at work. My boss was rude and the meeting was painful and frustrating beyond belief.",
      "The hotel room was disgusting and the staff were hostile. Everything about this place is miserable and ugly.",
      "The traffic situation in this city is dreadful. Commuting has become a painful and frustrating daily ordeal.",
      "Customer support was shocking and offensive. They were nasty and made the whole experience vile and unpleasant.",
      "The construction noise is annoying and the dust is terrible. Living here has become a miserable nightmare.",
      "This product is inferior in every way. The build quality is poor and it broke after one week. Worthless.",
      "The airline lost my luggage and was rude about it. A horrible and frustrating travel experience overall.",
      "The new policy changes are outrageous and cruel. Staff morale is at a desperate low and everyone is angry.",
      "This neighborhood has become ugly and unsafe. The abandoned buildings are hideous and the streets are gloomy.",
      "The exam results were disappointing and shocking. The grading system seems harsh and unfair to students.",
      "Dealing with this bureaucracy is painful and frustrating. Every interaction leaves me feeling miserable.",
      "The software update is terrible. It introduced annoying bugs and the interface is now ugly and confusing."
    ]
  end

  defp neutral_texts do
    [
      "The meeting has been scheduled for next Tuesday at three o'clock in the conference room on the second floor.",
      "According to the report, quarterly revenue increased by twelve percent compared to the previous fiscal year.",
      "The weather forecast predicts rain tomorrow morning with temperatures around fifty degrees and light winds.",
      "The new software update includes several bug fixes and performance improvements for the database module.",
      "The city council approved the budget for road repairs along the main corridor between downtown and the airport.",
      "Researchers published findings on migration patterns of arctic terns across the Atlantic Ocean last month.",
      "The library will extend its operating hours during finals week to accommodate students studying for exams.",
      "The train schedule has been updated for the holiday season with additional service on weekends and evenings.",
      "Annual inventory counts begin next Monday and all departments should prepare their asset documentation.",
      "The telecommunications company announced plans to expand fiber optic coverage to rural areas by next year.",
      "Registration for the spring semester opens on December first and students should consult their advisors.",
      "The parking structure on Fifth Avenue will be closed for maintenance during the first two weeks of January.",
      "Laboratory results from the water quality testing program indicate all samples meet federal safety standards.",
      "The committee reviewed proposals from three vendors for the new employee training platform contract."
    ]
  end

  # ---------------------------------------------------------------------------
  # Phase 1: Pipeline results
  # ---------------------------------------------------------------------------

  defp print_pipeline_results(outputs) do
    IO.puts("--- Pipeline Results ---\n")

    for {bucket, items} <- Enum.sort(outputs) do
      IO.puts("  #{bucket} (#{length(items)} items):")
      print_bucket_items(items)
    end

    IO.puts("")
  end

  defp print_bucket_items(items) do
    for item <- items do
      sentiment = item.payload["sentiment"]
      readability = item.payload["readability"]
      word_count = item.payload["word_count"]
      top_words = item.payload["top_words"]

      IO.puts("    sentiment: #{sentiment} | readability: #{readability} | words: #{word_count}")
      IO.puts("    top words: #{inspect(top_words)}")
      IO.puts("")
    end
  end

  # ---------------------------------------------------------------------------
  # Phase 2: Worker stats collection and merge
  # ---------------------------------------------------------------------------

  defp collect_and_print_worker_stats do
    IO.puts("--- Worker Isolation Proof ---\n")

    # Send stats requests to reach all workers in the pool.
    # Each worker has its own _global_index and _doc_count.
    probe_items = Item.wrap_many(for _ <- 1..4, do: "stats_probe")

    case fetch_worker_stats(probe_items) do
      {:ok, stats_results} ->
        worker_stats =
          stats_results
          |> Enum.map(& &1.payload)
          |> Enum.uniq_by(& &1["worker_id"])

        print_per_worker_stats(worker_stats)
        merged = merge_worker_stats(worker_stats)
        print_merged_stats(merged)

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
      max_in_flight: 4,
      on_error: :skip
    )
  end

  defp print_per_worker_stats(worker_stats) do
    IO.puts("  Per-worker accumulated state (each is a separate OS process):\n")

    for stats <- worker_stats do
      pid = stats["worker_id"]
      docs = stats["doc_count"]
      vocab = stats["vocab_size"]
      total = stats["total_word_occurrences"]

      IO.puts(
        "    Worker PID #{pid}: #{docs} docs processed, #{vocab} unique words, #{total} word occurrences"
      )
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
        - _global_index[word] += count from N threads = lost updates
          (read-modify-write race: two threads read the same value,
           both increment, both write back -- one increment is lost)
        - _doc_count += 1 from N threads = torn counter
          (same race: final count < actual number of documents)

      Under Slither:
        - Each Python worker is a separate OS process with its own memory
        - _global_index and _doc_count are process-local -- zero contention
        - The Elixir side merges per-worker results in a single process
        - No locks, no mutexes, no atomics -- just process isolation
        - Worker PIDs above prove these are genuinely separate processes
    """)
  end
end
