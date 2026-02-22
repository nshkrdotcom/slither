defmodule Slither.Store.Server do
  @moduledoc """
  GenServer owning ETS tables for a Store implementation.

  Tables are `:protected` — any BEAM process can read directly via ETS,
  but only this GenServer can write. This gives lock-free concurrent
  reads with serialized write consistency.
  """

  use GenServer
  require Logger

  alias Slither.{Store.Pointer, Telemetry}

  # --- Client API (Reads — bypass GenServer, go direct to ETS) ---

  @doc """
  Read a value from a store's table. Lock-free, no GenServer call.
  """
  @spec get(module(), atom(), term()) :: term() | nil
  def get(store_mod, table, key) do
    tid = Pointer.get(store_mod, table)

    case :ets.lookup(tid, key) do
      [{^key, value}] -> value
      [{^key} | _rest] = records -> records
      [] -> nil
    end
  end

  @doc """
  Read a value, raising if not found.
  """
  @spec get!(module(), atom(), term()) :: term()
  def get!(store_mod, table, key) do
    case get(store_mod, table, key) do
      nil -> raise KeyError, key: key, term: {store_mod, table}
      value -> value
    end
  end

  @doc """
  Match objects in a table. Lock-free.
  """
  @spec match(module(), atom(), term()) :: [tuple()]
  def match(store_mod, table, pattern) do
    tid = Pointer.get(store_mod, table)
    :ets.match_object(tid, pattern)
  end

  @doc """
  Select from a table using a match spec. Lock-free.
  """
  @spec select(module(), atom(), :ets.match_spec()) :: [term()]
  def select(store_mod, table, match_spec) do
    tid = Pointer.get(store_mod, table)
    :ets.select(tid, match_spec)
  end

  @doc """
  Fold over all entries in a table. Lock-free.
  """
  @spec fold(module(), atom(), acc, (tuple(), acc -> acc)) :: acc when acc: term()
  def fold(store_mod, table, acc, fun) do
    tid = Pointer.get(store_mod, table)
    :ets.foldl(fun, acc, tid)
  end

  @doc """
  Get the number of entries in a table. Lock-free.
  """
  @spec size(module(), atom()) :: non_neg_integer()
  def size(store_mod, table) do
    tid = Pointer.get(store_mod, table)
    :ets.info(tid, :size)
  end

  # --- Client API (Writes — serialized through GenServer) ---

  @doc """
  Insert a key-value pair into a table. Serialized.
  """
  @spec put(GenServer.server(), atom(), term(), term()) :: :ok
  def put(server, table, key, value) do
    GenServer.call(server, {:put, table, key, value})
  end

  @doc """
  Atomically read-modify-write a value. Serialized.
  """
  @spec update(GenServer.server(), atom(), term(), (term() -> term())) ::
          {:ok, term()} | {:error, :not_found}
  def update(server, table, key, fun) when is_function(fun, 1) do
    GenServer.call(server, {:update, table, key, fun})
  end

  @doc """
  Delete a key from a table. Serialized.
  """
  @spec delete(GenServer.server(), atom(), term()) :: :ok
  def delete(server, table, key) do
    GenServer.call(server, {:delete, table, key})
  end

  @doc """
  Bulk insert an enumerable of records into a table. Serialized.
  """
  @spec bulk_insert(GenServer.server(), atom(), Enumerable.t()) :: :ok
  def bulk_insert(server, table, records) do
    GenServer.call(server, {:bulk_insert, table, records}, :infinity)
  end

  @doc """
  Reload all tables by re-running the store's `load/1` callback.
  Creates new tables, loads data, then atomically swaps pointers.
  """
  @spec reload(GenServer.server()) :: :ok | {:error, term()}
  def reload(server) do
    GenServer.call(server, :reload, :infinity)
  end

  # --- Start ---

  def start_link(opts) do
    module = Keyword.fetch!(opts, :module)
    name = Keyword.get(opts, :name, module)
    GenServer.start_link(__MODULE__, module, name: name)
  end

  # --- GenServer Callbacks ---

  @impl true
  def init(module) do
    Process.flag(:trap_exit, true)
    tables = create_tables(module)

    # Load in the GenServer process (which owns the :protected ETS tables)
    # via handle_continue so we don't block the supervisor
    {:ok, %{module: module, tables: tables}, {:continue, :load}}
  end

  @impl true
  def handle_continue(:load, state) do
    try do
      state.module.load(state.tables)
      Logger.info("[Slither.Store] #{inspect(state.module)} loaded successfully")
    rescue
      e ->
        Logger.error(
          "[Slither.Store] #{inspect(state.module)} load failed: #{Exception.message(e)}"
        )
    end

    {:noreply, state}
  end

  @impl true
  def handle_call({:put, table, key, value}, _from, state) do
    tid = Pointer.get(state.module, table)
    :ets.insert(tid, {key, value})
    emit_write(state.module, table, 1)
    {:reply, :ok, state}
  end

  def handle_call({:update, table, key, fun}, _from, state) do
    tid = Pointer.get(state.module, table)

    result =
      case :ets.lookup(tid, key) do
        [{^key, value}] ->
          new_value = fun.(value)
          :ets.insert(tid, {key, new_value})
          {:ok, new_value}

        [] ->
          {:error, :not_found}
      end

    emit_write(state.module, table, 1)
    {:reply, result, state}
  end

  def handle_call({:delete, table, key}, _from, state) do
    tid = Pointer.get(state.module, table)
    :ets.delete(tid, key)
    emit_write(state.module, table, 1)
    {:reply, :ok, state}
  end

  def handle_call({:bulk_insert, table, records}, _from, state) do
    tid = Pointer.get(state.module, table)
    records_list = Enum.to_list(records)
    :ets.insert(tid, records_list)
    emit_write(state.module, table, length(records_list))
    {:reply, :ok, state}
  end

  def handle_call(:reload, _from, state) do
    result =
      Telemetry.span([:slither, :store, :reload], %{store: state.module}, fn ->
        old_tables = state.tables
        new_tables = create_tables(state.module)

        try do
          state.module.load(new_tables)
          retire_tables(old_tables)
          :ok
        rescue
          e ->
            Enum.each(new_tables, fn {_name, tid} -> :ets.delete(tid) end)
            {:error, Exception.message(e)}
        end
      end)

    case result do
      :ok -> {:reply, :ok, %{state | tables: create_table_map(state.module)}}
      error -> {:reply, error, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.tables, fn {name, tid} ->
      Pointer.delete(state.module, name)

      try do
        :ets.delete(tid)
      rescue
        _ -> :ok
      end
    end)
  end

  # --- Helpers ---

  defp emit_write(store, table, count) do
    Telemetry.emit([:slither, :store, :write], %{count: count}, %{store: store, table: table})
  end

  defp create_tables(module) do
    for spec <- module.tables(), into: %{} do
      tid =
        :ets.new(spec.name, [
          Map.get(spec, :type, :set),
          :protected,
          {:keypos, Map.get(spec, :keypos, 1)},
          {:read_concurrency, Map.get(spec, :read_concurrency, true)},
          {:write_concurrency, Map.get(spec, :write_concurrency, false)}
          | if(Map.get(spec, :compressed, false), do: [:compressed], else: [])
        ])

      Pointer.put(module, spec.name, tid)
      {spec.name, tid}
    end
  end

  defp create_table_map(module) do
    for spec <- module.tables(), into: %{} do
      {spec.name, Pointer.get(module, spec.name)}
    end
  end

  defp retire_tables(old_tables) do
    # Schedule deletion of old tables after a grace period
    # so in-flight reads using old tids can complete
    spawn(fn ->
      Process.sleep(5_000)

      Enum.each(old_tables, fn {_name, tid} ->
        try do
          :ets.delete(tid)
        rescue
          _ -> :ok
        end
      end)
    end)
  end
end
