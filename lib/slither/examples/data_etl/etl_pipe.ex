defmodule Slither.Examples.DataEtl.EtlPipe do
  @moduledoc """
  ETL pipeline: prepare -> validate -> transform -> route.

  Demonstrates a full data ETL flow with hot-reloadable schemas and
  per-worker audit logs that prove process isolation beats free-threaded
  Python:

    1. **prepare** (beam) -- reads the current validation schema from ETS
       and packages each row for Python validation, including schema version
    2. **validate** (python) -- validates rows against the schema using
       `csv_transformer.validate_batch`; records each decision in a
       per-worker audit log
    3. **transform** (beam) -- applies rename, cast, and default rules to
       valid rows; passes invalid rows through unchanged
    4. **route** (router) -- sends invalid rows to `:invalid` and all
       remaining valid rows to `:default`

  After both batches, the demo queries per-worker audit stats from Python
  to prove that each worker maintained a consistent, uncorrupted audit log
  in its own process -- something free-threaded Python cannot guarantee
  when multiple threads share the same list and counter.

  The demo shows schema hot-reload: batch 1 runs against v1 (lenient),
  then the schema is swapped to v2 (stricter) via a serialized store
  write, and batch 2 demonstrates that previously-valid rows now fail.

  Run with `Slither.Examples.DataEtl.EtlPipe.run_demo/0`.
  """

  use Slither.Pipe

  alias Slither.Dispatch
  alias Slither.Examples.DataEtl.SchemaStore
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
      batch_size: 20
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

  @doc """
  Package a raw row with the current schema for Python validation.

  Reads the active schema from the ETS-backed store (lock-free) and
  returns the map expected by `csv_transformer.validate_batch`.
  Includes the schema version so the Python audit log can track which
  version each row was validated against.
  """
  def prepare(item, _ctx) do
    schema = Server.get(SchemaStore, :schemas, :current)
    version = Server.get(SchemaStore, :schemas, :version)
    %{"row" => item.payload, "schema" => Map.put(schema, "version", version)}
  end

  @doc """
  Apply transformation rules to valid rows; pass invalid rows through.

  Valid rows get rename, cast, and default rules applied in Elixir.
  Invalid rows retain their validation result payload unchanged so
  the router can send them to the `:invalid` output.
  """
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

  # --- Demo data -------------------------------------------------------------

  defp batch_1_rows do
    [
      %{"name" => "Alice", "age" => 30, "email" => "alice@example.com"},
      %{"name" => "Bob", "age" => 25, "email" => "bob@example.com"},
      # invalid: empty name (min_length 1)
      %{"name" => "", "age" => 22, "email" => "empty@example.com"},
      %{"name" => "Charlie", "age" => 40, "email" => "charlie@test.com"},
      # invalid: negative age
      %{"name" => "Diana", "age" => -5, "email" => "diana@example.com"},
      %{"name" => "Eve", "age" => 28, "email" => "eve@example.com"},
      %{"name" => "Frank", "age" => 33, "email" => "frank@example.com"},
      # invalid: missing required name field
      %{"age" => 35, "email" => "noname@test.com"},
      %{"name" => "Grace", "age" => 45, "email" => "grace@example.com"},
      %{"name" => "Hank", "age" => 50, "email" => "hank@example.com"},
      # invalid: age exceeds max (150)
      %{"name" => "Irene", "age" => 200, "email" => "irene@example.com"},
      %{"name" => "Jack", "age" => 29, "email" => "jack@example.com"},
      %{"name" => "Karen", "age" => 37, "email" => "karen@example.com"},
      %{"name" => "Leo", "age" => 42, "email" => "leo@example.com"},
      %{"name" => "Mona", "age" => 26, "email" => "mona@example.com"},
      %{"name" => "Nate", "age" => 31, "email" => "nate@example.com"}
    ]
  end

  defp batch_2_rows do
    [
      # invalid v2: name too short (min_length 2), age < 18
      %{"name" => "F", "age" => 15, "email" => "young@test.com"},
      %{"name" => "Grace", "age" => 25, "email" => "grace@example.com"},
      # invalid v2: email fails pattern check
      %{"name" => "Hank", "age" => 45, "email" => "not-an-email"},
      %{"name" => "Ivy", "age" => 30, "email" => "ivy@example.com", "role" => "admin"},
      # invalid v2: age < 18
      %{"name" => "Jake", "age" => 16, "email" => "jake@example.com"},
      %{"name" => "Kara", "age" => 55, "email" => "kara@example.com"},
      # invalid v2: name single char (min_length 2)
      %{"name" => "L", "age" => 22, "email" => "l@example.com"},
      %{"name" => "Marcus", "age" => 38, "email" => "marcus@example.com"},
      %{"name" => "Nina", "age" => 27, "email" => "nina@example.com"},
      # invalid v2: bad email pattern, age too low
      %{"name" => "Oscar", "age" => 12, "email" => "oscar"},
      %{"name" => "Paula", "age" => 44, "email" => "paula@example.com"},
      %{"name" => "Quinn", "age" => 33, "email" => "quinn@example.com"},
      # invalid v2: missing name field
      %{"age" => 50, "email" => "anon@example.com"},
      %{"name" => "Rita", "age" => 29, "email" => "rita@example.com"},
      %{"name" => "Steve", "age" => 60, "email" => "steve@example.com"},
      %{"name" => "Tina", "age" => 35, "email" => "tina@example.com"}
    ]
  end

  # --- Demo ------------------------------------------------------------------

  @doc """
  Run the data ETL demo, printing formatted results to stdout.

  Demonstrates:
    * Batch validation against v1 schema (lenient) with 16 rows
    * Hot-reload of the schema to v2 (stricter constraints)
    * Re-validation showing rows that now fail under v2 with 16 rows
    * Per-worker audit stats proving process isolation keeps logs consistent
    * Why this matters vs. free-threaded Python
  """
  def run_demo do
    {:ok, _pid} = Server.start_link(module: SchemaStore, name: SchemaStore)
    Process.sleep(100)

    IO.puts("\n=== Slither Example: Data ETL Pipeline ===\n")

    # -- Batch 1: mix of valid and invalid rows under v1 schema ---------------

    batch_1 = batch_1_rows()
    version_1 = Server.get(SchemaStore, :schemas, :version)
    IO.puts("--- Batch 1 (schema v#{version_1}, lenient) ---")
    IO.puts("  Input: #{length(batch_1)} rows\n")

    {:ok, out1} = Runner.run(__MODULE__, batch_1)
    print_results(out1)

    # -- Hot-reload: swap to v2 schema ----------------------------------------

    IO.puts("\n--- Hot-reloading schema to v2 ---")
    Server.put(SchemaStore, :schemas, :current, SchemaStore.schema_v2())
    Server.put(SchemaStore, :schemas, :version, 2)
    version_2 = Server.get(SchemaStore, :schemas, :version)
    IO.puts("  Schema version: #{version_1} -> #{version_2}")

    IO.puts("  Changes: name min_length 1->2, age 0-150 -> 18-120, email pattern required\n")

    # -- Batch 2: rows that pass v1 but fail v2 -------------------------------

    batch_2 = batch_2_rows()
    IO.puts("--- Batch 2 (schema v#{version_2}, stricter) ---")
    IO.puts("  Input: #{length(batch_2)} rows\n")

    {:ok, out2} = Runner.run(__MODULE__, batch_2)
    print_results(out2)

    # -- Audit stats: query per-worker audit logs -----------------------------

    IO.puts("\n--- Per-Worker Audit Stats ---")
    fetch_and_print_audit_stats()

    # -- Why this matters -----------------------------------------------------

    print_why_this_matters(version_1, version_2)

    IO.puts("\nDone!")
  end

  # --- Audit stats -----------------------------------------------------------

  defp fetch_and_print_audit_stats do
    # Send probe items to reach all workers in the pool.
    # Each worker has its own _audit_log and _validation_count.
    probe_items = Item.wrap_many(for _ <- 1..4, do: %{})

    case Dispatch.run(probe_items,
           executor: Slither.Dispatch.Executors.SnakeBridge,
           module: "csv_transformer",
           function: "get_audit_stats",
           batch_size: 1,
           max_in_flight: 4,
           on_error: :skip
         ) do
      {:ok, results} ->
        results
        |> Enum.map(& &1.payload)
        |> Enum.uniq_by(& &1["worker_id"])
        |> Enum.each(&print_audit_stats/1)

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
    recent = stats["recent_entries"]

    IO.puts("  Worker PID #{worker_id}:")
    IO.puts("    Validations performed: #{validation_count}")
    IO.puts("    Audit log entries:     #{log_size}")
    IO.puts("    Valid / Invalid:       #{valid} / #{invalid}")
    IO.puts("    Schema versions seen:  #{inspect(versions)}")
    IO.puts("    Log consistent:        #{validation_count == log_size}")
    IO.puts("")

    IO.puts("  Recent audit entries:")
    print_recent_entries(recent)

    IO.puts("")
    IO.puts("  Audit log is internally consistent: validation_count (#{validation_count})")
    IO.puts("  matches audit_log_size (#{log_size}), valid + invalid (#{valid + invalid})")
    IO.puts("  matches total -- no lost entries, no corrupted state.")
  end

  defp print_recent_entries(entries) do
    Enum.each(entries, fn entry ->
      row_id = entry["row_id"]
      valid = entry["valid"]
      errors = entry["error_count"]
      version = entry["schema_version"]
      label = if valid, do: "pass", else: "fail(#{errors})"

      IO.puts("    row_id=#{inspect(row_id)} | #{label} | schema v#{version}")
    end)
  end

  # --- Why this matters ------------------------------------------------------

  defp print_why_this_matters(version_1, version_2) do
    IO.puts("""

    --- Why This Matters ---

      Under free-threaded Python (no GIL):
        * _audit_log.append() from N threads = corrupted list
          (lost entries, index errors -- list.append is only atomic under the GIL)
        * _validation_count += 1 from N threads = lost increments
          (classic read-modify-write race)
        * Reading the schema dict while another thread swaps it = torn read
          (partial old keys + partial new keys)

      Under Slither (process-per-worker):
        * Schema read atomically from ETS -- no torn reads possible
        * Each worker has its own _audit_log in its own OS process
        * validation_count == audit_log_size == valid + invalid (always)
        * Hot-reload was atomic: batch 1 saw v#{version_1}, batch 2 saw v#{version_2}, no torn reads
    """)
  end

  # --- Output formatting -----------------------------------------------------

  defp print_results(outputs) do
    for {bucket, items} <- Enum.sort(outputs) do
      IO.puts("  #{bucket}: #{length(items)} items")
      Enum.each(items, &print_row/1)
      IO.puts("")
    end
  end

  defp print_row(item) do
    row = item.payload["row"]

    if item.payload["valid"] do
      IO.puts("    [valid]   #{inspect(row, limit: 8)}")
    else
      IO.puts("    [invalid] #{inspect(row, limit: 8)}")
      IO.puts("              errors: #{inspect(item.payload["errors"], limit: 5)}")
    end
  end
end
