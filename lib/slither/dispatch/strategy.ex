defmodule Slither.Dispatch.Strategy do
  @moduledoc """
  Behaviour for pluggable batching strategies.

  A strategy determines how items are grouped into batches
  before being sent to an executor.
  """

  @doc """
  Split a list of items into batches according to this strategy's rules.

  Returns a list of `{batch_ref, batch}` tuples where `batch_ref` is a
  monotonically increasing integer for ordering.
  """
  @callback batch([Slither.Item.t()], keyword()) ::
              [{batch_ref :: pos_integer(), batch :: [Slither.Item.t()]}]
end
