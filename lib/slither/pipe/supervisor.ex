defmodule Slither.Pipe.Supervisor do
  @moduledoc """
  DynamicSupervisor for long-lived pipe topologies.

  Short-lived pipe runs execute directly via `Slither.Pipe.Runner.run/3`.
  This supervisor is for cases where a pipe needs to run continuously
  (e.g., processing a stream of events).
  """

  use DynamicSupervisor

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
