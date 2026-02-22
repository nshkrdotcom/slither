defmodule Slither.Dispatch.Executor do
  @moduledoc """
  Behaviour for executing a batch of items against an external runtime.

  Dispatch does not hardcode how Python is called. Instead, it delegates
  to an Executor implementation that handles the actual bridge call.
  """

  @doc """
  Execute a batch of items and return results in the same order.
  """
  @callback execute_batch([Slither.Item.t()], Slither.Context.t(), keyword()) ::
              {:ok, [Slither.Item.t()]} | {:error, term()}
end
