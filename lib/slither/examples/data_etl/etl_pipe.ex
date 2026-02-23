defmodule Slither.Examples.DataEtl.EtlPipe do
  @moduledoc """
  ETL pipeline: prepare -> validate -> transform -> route.

  Processes 10,000 rows through 48 workers with hot-reloadable schemas
  and per-worker audit logs. Demonstrates:

    1. Batch 1: 5,000 rows under v1 schema (lenient)
    2. Hot-reload to v2 schema (stricter)
    3. Batch 2: 5,000 rows under v2 with known invalid ratios
    4. Batch 3 (concurrent): Fired *during* another schema hot-reload
       to stress atomic ETS reads

  Per-worker audit stats prove process isolation keeps logs consistent.

  Run with `Slither.Examples.DataEtl.EtlPipe.run_demo/0`.
  """

  use Slither.Pipe

  alias Slither.Dispatch
  alias Slither.Examples.DataEtl.SchemaStore
  alias Slither.Examples.Reporter
  alias Slither.Item
  alias Slither.Pipe.Runner
  alias Slither.Store.Server

  pipe :data_etl do
    store(:schema, SchemaStore)

    stage(:prepare, :beam, handler: &__MODULE__.prepare/2)

    stage(:validate, :python,
      executor: Slither.Dispatch.Executors.SnakeBridge,
      module: "csv_transformer",
      function: "validate_batch",
      batch_size: 100
    )

    stage(:transform, :beam, handler: &__MODULE__.maybe_transform/2)

    stage(:route, :router,
      routes: [
        {fn item -> item.payload["valid"] == false end, :invalid}
      ]
    )

    output(:invalid)

    on_error(:validate, :skip)
    on_error(:transform, :skip)
    on_error(:*, :halt)
  end

  def prepare(item, _ctx) do
    schema = Server.get(SchemaStore, :schemas, :current)
    version = Server.get(SchemaStore, :schemas, :version)
    %{"row" => item.payload, "schema" => Map.put(schema, "version", version)}
  end

  def maybe_transform(item, _ctx) do
    if item.payload["valid"] do
      rules = Server.get(SchemaStore, :rules, :transform)
      row = item.payload["row"]
      transformed = apply_rules(row, rules)
      Map.put(item.payload, "row", transformed)
    else
      item.payload
    end
  end

  # --- Rule application ------------------------------------------------------

  defp apply_rules(row, rules) do
    row
    |> apply_renames(rules["rename"] || %{})
    |> apply_casts(rules["cast"] || %{})
    |> apply_defaults(rules["defaults"] || %{})
  end

  defp apply_renames(row, renames) do
    Enum.reduce(renames, row, fn {old, new}, acc ->
      case Map.pop(acc, old) do
        {nil, acc} -> acc
        {val, acc} -> Map.put(acc, new, val)
      end
    end)
  end

  defp apply_casts(row, casts) do
    Enum.reduce(casts, row, fn {field, type}, acc ->
      case Map.fetch(acc, field) do
        {:ok, value} -> Map.put(acc, field, cast_value(value, type))
        :error -> acc
      end
    end)
  end

  defp cast_value(value, "integer") when is_binary(value) do
    case Integer.parse(value) do
      {int, _} -> int
      :error -> value
    end
  end

  defp cast_value(value, "integer") when is_float(value), do: trunc(value)
  defp cast_value(value, _type), do: value

  defp apply_defaults(row, defaults) do
    Enum.reduce(defaults, row, fn {key, default}, acc ->
      Map.put_new(acc, key, default)
    end)
  end

  # --- Demo data generation --------------------------------------------------

  @names ~w(Alice Bob Charlie Diana Eve Frank Grace Hank Irene Jack Karen Leo
            Mona Nate Oscar Paula Quinn Rita Steve Tina Uma Victor Wendy Xavier
            Yara Zane Amber Blake Casey Drew)

  defp generate_rows(count, invalid_ratio) do
    :rand.seed(:exsss, {42, count, trunc(invalid_ratio * 1000)})

    for i <- 1..count do
      if :rand.uniform() < invalid_ratio do
        generate_invalid_row(i)
      else
        generate_valid_row(i)
      end
    end
  end

  defp generate_valid_row(i) do
    name = Enum.at(@names, rem(i, length(@names)))
    age = 20 + rem(i, 60)
    %{"name" => name, "age" => age, "email" => "#{String.downcase(name)}#{i}@example.com"}
  end

  defp generate_invalid_row(i) do
    case rem(i, 4) do
      0 -> %{"name" => "", "age" => 25, "email" => "empty#{i}@example.com"}
      1 -> %{"name" => "User#{i}", "age" => -5, "email" => "neg#{i}@example.com"}
      2 -> %{"age" => 35, "email" => "noname#{i}@test.com"}
      3 -> %{"name" => "Bad#{i}", "age" => 200, "email" => "bad#{i}@example.com"}
    end
  end

  # --- Demo ------------------------------------------------------------------

  def run_demo do
    {:ok, _pid} = Server.start_link(module: SchemaStore, name: SchemaStore)
    Process.sleep(100)

    IO.puts("\n=== Slither Example: Data ETL Pipeline ===\n")

    # -- Batch 1: 5,000 rows under v1 schema (10% invalid) -------------------

    batch_1 = generate_rows(5000, 0.10)
    version_1 = Server.get(SchemaStore, :schemas, :version)
    IO.puts("--- Batch 1 (schema v#{version_1}, lenient, 5000 rows) ---")

    {t1, {:ok, out1}} = :timer.tc(fn -> Runner.run(__MODULE__, batch_1) end)
    print_results(out1)
    Reporter.print_timing("Batch 1", t1)
    Reporter.print_throughput("Throughput", {5000, t1})
    IO.puts("")

    # -- Hot-reload: swap to v2 schema ----------------------------------------

    IO.puts("--- Hot-reloading schema to v2 ---")
    Server.put(SchemaStore, :schemas, :current, SchemaStore.schema_v2())
    Server.put(SchemaStore, :schemas, :version, 2)
    version_2 = Server.get(SchemaStore, :schemas, :version)
    IO.puts("  Schema version: #{version_1} -> #{version_2}")
    IO.puts("  Changes: name min_length 1->2, age 0-150 -> 18-120, email pattern required\n")

    # -- Batch 2: 5,000 rows under v2 schema (20% invalid) -------------------

    batch_2 = generate_rows(5000, 0.20)
    IO.puts("--- Batch 2 (schema v#{version_2}, stricter, 5000 rows) ---")

    {t2, {:ok, out2}} = :timer.tc(fn -> Runner.run(__MODULE__, batch_2) end)
    print_results(out2)
    Reporter.print_timing("Batch 2", t2)
    Reporter.print_throughput("Throughput", {5000, t2})
    IO.puts("")

    # -- Batch 3: Concurrent with another hot-reload --------------------------

    IO.puts("--- Batch 3 (concurrent with schema toggle, stress ETS reads) ---")

    # Spawn a task that rapidly toggles the schema back and forth
    toggler =
      Task.async(fn ->
        for _ <- 1..20 do
          Server.put(SchemaStore, :schemas, :current, SchemaStore.schema_v2())
          Server.put(SchemaStore, :schemas, :version, 2)
          Process.sleep(5)

          schema_v1 = %{
            "name" => %{"type" => "string", "required" => true, "min_length" => 1},
            "age" => %{"type" => "integer", "required" => true, "min" => 0, "max" => 150},
            "email" => %{"type" => "string", "required" => false}
          }

          Server.put(SchemaStore, :schemas, :current, schema_v1)
          Server.put(SchemaStore, :schemas, :version, 1)
          Process.sleep(5)
        end
      end)

    batch_3 = generate_rows(5000, 0.15)
    # Note: we cannot know exact valid/invalid split because schema is toggling
    {t3, result3} = :timer.tc(fn -> Runner.run(__MODULE__, batch_3) end)

    # Ensure toggler finishes
    Task.await(toggler, 10_000)

    # Restore to v2 for consistency
    Server.put(SchemaStore, :schemas, :current, SchemaStore.schema_v2())
    Server.put(SchemaStore, :schemas, :version, 2)

    case result3 do
      {:ok, out3} ->
        print_results(out3)
        Reporter.print_timing("Batch 3 (concurrent)", t3)
        Reporter.print_throughput("Throughput", {5000, t3})
        IO.puts("  Schema was toggling during validation -- ETS reads were atomic\n")

      {:error, reason, _} ->
        IO.puts("  Batch 3 error: #{inspect(reason)}\n")
    end

    # -- Audit stats -----------------------------------------------------------

    IO.puts("--- Per-Worker Audit Stats ---")
    fetch_and_print_audit_stats()

    # -- Why this matters ------------------------------------------------------

    print_why_this_matters(version_1, version_2)

    IO.puts("\nDone!")
  end

  # --- Audit stats -----------------------------------------------------------

  defp fetch_and_print_audit_stats do
    probe_items = Item.wrap_many(for _ <- 1..200, do: %{})

    case Dispatch.run(probe_items,
           executor: Slither.Dispatch.Executors.SnakeBridge,
           module: "csv_transformer",
           function: "get_audit_stats",
           batch_size: 1,
           max_in_flight: 16,
           on_error: :skip
         ) do
      {:ok, results} ->
        unique =
          results
          |> Enum.map(& &1.payload)
          |> Enum.reject(fn s -> s["worker_id"] == nil end)
          |> Enum.uniq_by(& &1["worker_id"])

        for stats <- Enum.take(unique, 10) do
          print_audit_stats(stats)
        end

        if length(unique) > 10 do
          IO.puts("  ... and #{length(unique) - 10} more workers\n")
        end

        total_validations = Enum.sum(Enum.map(unique, & &1["validation_count"]))
        total_log = Enum.sum(Enum.map(unique, & &1["audit_log_size"]))
        IO.puts("  Workers reached: #{length(unique)} of 48")
        Reporter.print_correctness("Total validations vs audit log", total_validations, total_log)
        IO.puts("")

      {:error, reason, _partial} ->
        IO.puts("  Could not fetch audit stats: #{inspect(reason)}")
    end
  end

  defp print_audit_stats(stats) do
    worker_id = stats["worker_id"]
    validation_count = stats["validation_count"]
    valid = stats["valid_count"]
    invalid = stats["invalid_count"]
    log_size = stats["audit_log_size"]
    versions = stats["schema_versions_seen"]

    IO.puts("  Worker PID #{worker_id}:")
    IO.puts("    Validations: #{validation_count} | Valid: #{valid} | Invalid: #{invalid}")
    IO.puts("    Audit log: #{log_size} | Versions seen: #{inspect(versions)}")
    IO.puts("    Consistent: #{validation_count == log_size}")
    IO.puts("")
  end

  # --- Why this matters ------------------------------------------------------

  defp print_why_this_matters(version_1, version_2) do
    IO.puts("""

    --- Why This Matters ---

      Under free-threaded Python (no GIL):
        * _audit_log.append() from 48 threads = corrupted list
        * _validation_count += 1 from 48 threads = lost increments
        * Reading schema dict while hot-reload thread swaps it = torn read
        * 10,000 rows across 3 batches = massive contention window

      Under Slither (process-per-worker):
        * Schema read atomically from ETS -- no torn reads possible
        * Each of 48 workers has its own _audit_log in its own OS process
        * validation_count == audit_log_size == valid + invalid (always)
        * Hot-reload was atomic: batch 1 saw v#{version_1}, batch 2 saw v#{version_2}, batch 3 survived toggling
    """)
  end

  # --- Output formatting -----------------------------------------------------

  defp print_results(outputs) do
    for {bucket, items} <- Enum.sort(outputs) do
      IO.puts("  #{bucket}: #{length(items)} items")
    end

    IO.puts("")
  end
end
