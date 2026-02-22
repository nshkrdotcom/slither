defmodule Slither.Store do
  @moduledoc """
  Behaviour for ETS-backed shared state with Python-accessible views.

  A Store declares tables, their schemas, views for Python access,
  and a loader for initial data population. Tables use `:protected`
  access so reads are lock-free from any BEAM process while writes
  are serialized through the owning GenServer.

  ## Example

      defmodule MyApp.FeatureStore do
        @behaviour Slither.Store

        @impl true
        def tables do
          [
            %{name: :features, type: :set, read_concurrency: true},
            %{name: :user_index, type: :ordered_set, read_concurrency: true}
          ]
        end

        @impl true
        def views do
          [
            %{
              name: :lookup_feature,
              mode: :scalar,
              scope: :session,
              handler: fn %{"key" => key}, _ctx ->
                case Slither.Store.Server.get(MyApp.FeatureStore, :features, key) do
                  nil -> %{"error" => "not_found"}
                  value -> %{"value" => value}
                end
              end
            },
            %{
              name: :lookup_features_batch,
              mode: :batch,
              scope: :session,
              handler: fn %{"keys" => keys}, _ctx ->
                results = Enum.map(keys, &Slither.Store.Server.get(MyApp.FeatureStore, :features, &1))
                %{"results" => results}
              end
            }
          ]
        end

        @impl true
        def load(tables) do
          # Populate tables from your data source
          data = load_from_database()
          Enum.each(data, fn {key, value} ->
            :ets.insert(tables[:features], {key, value})
          end)
          :ok
        end
      end
  """

  @type table_name :: atom()

  @type table_spec :: %{
          name: table_name(),
          type: :set | :bag | :ordered_set,
          keypos: pos_integer(),
          read_concurrency: boolean(),
          write_concurrency: boolean(),
          compressed: boolean()
        }

  @type view_mode :: :scalar | :batch
  @type view_scope :: :session | :global

  @type view_spec :: %{
          name: atom(),
          mode: view_mode(),
          scope: view_scope(),
          handler: (map(), Slither.Context.t() -> map()),
          timeout_ms: non_neg_integer() | :infinity
        }

  @doc "Declare tables and their schemas."
  @callback tables() :: [table_spec()]

  @doc "Declare Python-accessible view functions."
  @callback views() :: [view_spec()]

  @doc "Load initial data into the tables. Called once at startup."
  @callback load(tabs :: %{table_name() => :ets.tid()}) :: :ok
end
