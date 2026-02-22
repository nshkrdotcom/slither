defmodule Slither.Store.Supervisor do
  @moduledoc """
  Supervisor for all configured Store processes.
  """

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    stores = Keyword.get(opts, :stores, [])

    children =
      Enum.map(stores, fn store_module ->
        {Slither.Store.Server, module: store_module}
      end)

    Supervisor.init(children, strategy: :one_for_one)
  end
end
