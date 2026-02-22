defmodule Slither.Stage.Beam do
  @moduledoc """
  Stage that runs an Elixir function.

  Wraps a handler function as a pipeline stage. Supports both
  item-level and batch-level processing.

  ## Options

    * `:handler` - Function `(Slither.Item.t(), Slither.Context.t() -> term())`
      that processes a single item. The return value becomes the item's new payload.
      Can also return `{:ok, payload}`, `{:skip, reason}`, `{:error, reason}`,
      or `{:route, dest, payload}`.
    * `:batch_handler` - Optional function for batch processing.
    * `:cardinality` - `:one` (default) or `:many`. When `:many`, the handler
      returns a list and each element becomes a separate downstream item.
  """

  alias Slither.Item

  @behaviour Slither.Stage

  @impl true
  def init(opts) do
    handler = Keyword.fetch!(opts, :handler)
    batch_handler = Keyword.get(opts, :batch_handler)
    cardinality = Keyword.get(opts, :cardinality, :one)

    {:ok, %{handler: handler, batch_handler: batch_handler, cardinality: cardinality}}
  end

  @impl true
  def handle_item(item, ctx, state) do
    result = state.handler.(item, ctx)
    normalize_result(item, result, state.cardinality)
  rescue
    e -> {:error, {e, __STACKTRACE__}}
  end

  @impl true
  def handle_batch(items, ctx, state) do
    if state.batch_handler do
      state.batch_handler.(items, ctx)
    else
      # Delegate to per-item processing. Return :per_item so the
      # pipe runner can apply its error policy per item.
      :per_item
    end
  end

  @impl true
  def shutdown(_state), do: :ok

  # --- Internal ---

  defp normalize_result(item, {:ok, payload}, :many) when is_list(payload) do
    {:ok, expand_many(payload, item.meta)}
  end

  defp normalize_result(item, {:ok, payload}, _cardinality) do
    {:ok, [Item.update_payload(item, payload)]}
  end

  defp normalize_result(_item, {:skip, reason}, _cardinality) do
    {:skip, reason}
  end

  defp normalize_result(_item, {:error, reason}, _cardinality) do
    {:error, reason}
  end

  defp normalize_result(item, {:route, dest, payload}, _cardinality) do
    {:route, dest, [Item.update_payload(item, payload)]}
  end

  defp normalize_result(item, payload, :many) when is_list(payload) do
    {:ok, expand_many(payload, item.meta)}
  end

  defp normalize_result(item, payload, _cardinality) do
    {:ok, [Item.update_payload(item, payload)]}
  end

  defp expand_many(payloads, meta) do
    Enum.map(payloads, fn p -> p |> Item.wrap() |> Item.put_meta(meta) end)
  end
end
