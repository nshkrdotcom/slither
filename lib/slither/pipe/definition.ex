defmodule Slither.Pipe.Definition do
  @moduledoc """
  Normalized, compiled representation of a pipe definition.

  Created by the `Slither.Pipe.Builder` DSL macros and consumed
  by `Slither.Pipe.Runner` at execution time.
  """

  defstruct name: nil,
            stages: [],
            stores: %{},
            outputs: MapSet.new([:default]),
            error_policy: %{},
            runtime: %{}

  @type stage_entry :: %{
          name: atom(),
          type: :beam | :python | :router,
          opts: keyword()
        }

  @type t :: %__MODULE__{
          name: atom(),
          stages: [stage_entry()],
          stores: %{atom() => module()},
          outputs: MapSet.t(atom()),
          error_policy: %{atom() => Slither.Dispatch.on_error()},
          runtime: map()
        }

  @doc """
  Get the stage module for a given stage type.
  """
  @spec stage_module(:beam | :python | :router) :: module()
  def stage_module(:beam), do: Slither.Stage.Beam
  def stage_module(:python), do: Slither.Stage.Python
  def stage_module(:router), do: Slither.Stage.Router

  @doc """
  Get the error policy for a specific stage, falling back to the wildcard policy.
  """
  @spec error_policy_for(t(), atom()) :: Slither.Dispatch.on_error()
  def error_policy_for(%__MODULE__{} = def, stage_name) do
    Map.get(def.error_policy, stage_name) ||
      Map.get(def.error_policy, :*, :halt)
  end
end
