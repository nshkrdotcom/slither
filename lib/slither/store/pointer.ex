defmodule Slither.Store.Pointer do
  @moduledoc """
  Indirection layer for hot-swap reload of ETS tables.

  Maps `{store_module, table_name}` to the current `:ets.tid()` via
  `:persistent_term`. On reload, a new table is fully populated
  before the pointer is swapped, so readers never see partial state.
  """

  @doc """
  Get the current ETS table ID for a store's table.
  """
  @spec get(module(), atom()) :: :ets.tid()
  def get(store_mod, table) do
    :persistent_term.get({__MODULE__, store_mod, table})
  end

  @doc """
  Set the ETS table ID for a store's table.
  """
  @spec put(module(), atom(), :ets.tid()) :: :ok
  def put(store_mod, table, tid) do
    :persistent_term.put({__MODULE__, store_mod, table}, tid)
    :ok
  end

  @doc """
  Remove the pointer for a store's table.
  """
  @spec delete(module(), atom()) :: :ok
  def delete(store_mod, table) do
    :persistent_term.erase({__MODULE__, store_mod, table})
    :ok
  rescue
    ArgumentError -> :ok
  end
end
