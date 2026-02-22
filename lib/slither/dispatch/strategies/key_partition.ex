defmodule Slither.Dispatch.Strategies.KeyPartition do
  @moduledoc """
  Partition items by a key function, one batch per unique key.
  Useful when items with the same key must be processed together.

  ## Options

    * `:key_fn` - Function `(Slither.Item.t() -> term())` extracting the
      partition key from an item (required)
  """

  @behaviour Slither.Dispatch.Strategy

  @impl true
  def batch(items, opts) do
    key_fn = Keyword.fetch!(opts, :key_fn)

    items
    |> Enum.group_by(key_fn)
    |> Enum.sort_by(fn {key, _} -> key end)
    |> Enum.with_index(1)
    |> Enum.map(fn {{_key, group}, ref} -> {ref, group} end)
  end
end
