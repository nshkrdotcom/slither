defmodule Slither.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Task supervisor for store loaders
      {Task.Supervisor, name: Slither.LoaderSupervisor},

      # View registry for tracking bridge callbacks per session
      Slither.Bridge.ViewRegistry,

      # Store supervisor — starts all configured stores
      {Slither.Store.Supervisor, stores: Application.get_env(:slither, :stores, [])},

      # Pipe supervisor — for long-running topologies
      Slither.Pipe.Supervisor
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Slither.Supervisor)
  end
end
