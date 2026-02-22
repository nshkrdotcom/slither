defmodule Slither.Bridge.ViewRegistry do
  @moduledoc """
  ETS-backed registry mapping session IDs to registered view callback IDs.

  Tracks which SnakeBridge callbacks belong to which Slither session
  so they can be cleaned up when a session ends.
  """

  use GenServer

  @table __MODULE__

  # --- Client API ---

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Register a view callback for a session (or :global).
  """
  @spec register(binary() | :global, atom(), binary()) :: :ok
  def register(scope, view_name, callback_id) do
    :ets.insert(@table, {scope, view_name, callback_id})
    :ok
  end

  @doc """
  Get all callback registrations for a session.
  """
  @spec get_session_callbacks(binary() | :global) :: [{atom(), binary()}]
  def get_session_callbacks(scope) do
    @table
    |> :ets.match({scope, :"$1", :"$2"})
    |> Enum.map(fn [name, id] -> {name, id} end)
  end

  @doc """
  Remove all registrations for a session.
  """
  @spec clear_session(binary() | :global) :: :ok
  def clear_session(scope) do
    :ets.match_delete(@table, {scope, :_, :_})
    :ok
  end

  # --- GenServer ---

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:named_table, :bag, :public, read_concurrency: true])
    {:ok, %{table: table}}
  end
end
