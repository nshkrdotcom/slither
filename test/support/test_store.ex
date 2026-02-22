defmodule Slither.TestStore do
  @moduledoc """
  Simple store implementation for testing.
  """

  alias Slither.Store.Server

  @behaviour Slither.Store

  @impl true
  def tables do
    [
      %{
        name: :test_data,
        type: :set,
        read_concurrency: true,
        write_concurrency: false,
        keypos: 1,
        compressed: false
      }
    ]
  end

  @impl true
  def views do
    [
      %{
        name: :test_lookup,
        mode: :scalar,
        scope: :session,
        handler: fn %{"key" => key}, _ctx ->
          case Server.get(__MODULE__, :test_data, key) do
            nil -> %{"error" => "not_found"}
            value -> %{"value" => value}
          end
        end,
        timeout_ms: 5_000
      }
    ]
  end

  @impl true
  def load(tables) do
    data = [
      {"key1", "value1"},
      {"key2", "value2"},
      {"key3", "value3"}
    ]

    Enum.each(data, fn {k, v} ->
      :ets.insert(tables[:test_data], {k, v})
    end)

    :ok
  end
end
