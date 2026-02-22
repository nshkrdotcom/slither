defmodule Slither.Dispatch.Strategies.FixedBatch do
  @moduledoc """
  Split items into fixed-size chunks.

  ## Options

    * `:batch_size` - Number of items per batch (default: 64)
  """

  @behaviour Slither.Dispatch.Strategy

  @impl true
  def batch(items, opts) do
    batch_size = Keyword.get(opts, :batch_size, 64)

    items
    |> Enum.chunk_every(batch_size)
    |> Enum.with_index(1)
    |> Enum.map(fn {chunk, ref} -> {ref, chunk} end)
  end
end
