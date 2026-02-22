defmodule Slither.Dispatch.Strategies.WeightedBatch do
  @moduledoc """
  Batch items by a weight function, splitting when cumulative weight
  exceeds a threshold. Useful for token-count or byte-size batching.

  ## Options

    * `:max_weight` - Maximum cumulative weight per batch (default: 1000)
    * `:weight_fn` - Function `(Slither.Item.t() -> number())` returning
      the weight of an item (default: `fn _ -> 1 end`)
  """

  @behaviour Slither.Dispatch.Strategy

  @impl true
  def batch(items, opts) do
    max_weight = Keyword.get(opts, :max_weight, 1000)
    weight_fn = Keyword.get(opts, :weight_fn, fn _ -> 1 end)

    {batches, current, _weight, ref} =
      Enum.reduce(items, {[], [], 0, 1}, fn item, {batches, current, weight, ref} ->
        item_weight = weight_fn.(item)

        if weight + item_weight > max_weight and current != [] do
          {[{ref, Enum.reverse(current)} | batches], [item], item_weight, ref + 1}
        else
          {batches, [item | current], weight + item_weight, ref}
        end
      end)

    final =
      if current != [] do
        [{ref, Enum.reverse(current)} | batches]
      else
        batches
      end

    Enum.reverse(final)
  end
end
