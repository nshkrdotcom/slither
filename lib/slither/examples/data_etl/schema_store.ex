defmodule Slither.Examples.DataEtl.SchemaStore do
  @moduledoc """
  Hot-reloadable schema store for data validation.

  Holds versioned validation schemas and transformation rules in ETS
  tables with read concurrency enabled. Supports live schema updates
  via `Slither.Store.Server.put/4` to demonstrate hot-reload without
  process restart.

  Ships with two schema versions:

    * **v1** -- baseline schema with name, age (0..150), and email
    * **v2** -- stricter rules: name min-length 2, age 18..120,
      email pattern validation, and an optional role field

  Run the ETL pipeline demo with
  `Slither.Examples.DataEtl.EtlPipe.run_demo/0`.
  """

  @behaviour Slither.Store

  @schema_v1 %{
    "fields" => %{
      "name" => %{"type" => "string", "required" => true, "min_length" => 1},
      "age" => %{"type" => "integer", "required" => true, "min" => 0, "max" => 150},
      "email" => %{"type" => "string", "required" => true}
    }
  }

  @schema_v2 %{
    "fields" => %{
      "name" => %{"type" => "string", "required" => true, "min_length" => 2},
      "age" => %{"type" => "integer", "required" => true, "min" => 18, "max" => 120},
      "email" => %{
        "type" => "string",
        "required" => true,
        "pattern" => "^[^@]+@[^@]+$"
      },
      "role" => %{"type" => "string", "required" => false}
    }
  }

  @transform_rules %{
    "rename" => %{"name" => "full_name"},
    "cast" => %{"age" => "integer"},
    "defaults" => %{"role" => "user"}
  }

  # --- Public helpers --------------------------------------------------------

  @doc "Return the v1 validation schema map."
  @spec schema_v1() :: map()
  def schema_v1, do: @schema_v1

  @doc "Return the v2 validation schema map."
  @spec schema_v2() :: map()
  def schema_v2, do: @schema_v2

  @doc "Return the default transformation rules map."
  @spec transform_rules() :: map()
  def transform_rules, do: @transform_rules

  # --- Store behaviour -------------------------------------------------------

  @impl true
  def tables do
    [
      %{
        name: :schemas,
        type: :set,
        keypos: 1,
        read_concurrency: true,
        write_concurrency: false,
        compressed: false
      },
      %{
        name: :rules,
        type: :set,
        keypos: 1,
        read_concurrency: true,
        write_concurrency: false,
        compressed: false
      }
    ]
  end

  @impl true
  def views, do: []

  @impl true
  def load(tables) do
    :ets.insert(tables[:schemas], {:current, @schema_v1})
    :ets.insert(tables[:schemas], {:version, 1})
    :ets.insert(tables[:rules], {:transform, @transform_rules})
    :ok
  end
end
