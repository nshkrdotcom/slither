defmodule Slither.Stage.Router do
  @moduledoc """
  Stage that routes items to named outputs based on predicates.

  Routes are evaluated in order; the first matching predicate determines
  the destination. Items that match no route go to `:default`.

  ## Options

    * `:routes` - List of `{predicate, destination}` tuples where
      `predicate` is `(Slither.Item.t() -> boolean())` and
      `destination` is an atom.
  """

  @behaviour Slither.Stage

  @impl true
  def init(opts) do
    routes = Keyword.fetch!(opts, :routes)
    {:ok, %{routes: routes}}
  end

  @impl true
  def handle_item(item, _ctx, state) do
    dest = find_route(item, state.routes)
    {:route, dest, [item]}
  end

  @impl true
  def handle_batch(items, _ctx, state) do
    routed =
      Enum.reduce(items, %{}, fn item, acc ->
        dest = find_route(item, state.routes)
        Map.update(acc, dest, [item], &[item | &1])
      end)

    # Reverse to preserve order within each destination
    routed = Map.new(routed, fn {dest, items} -> {dest, Enum.reverse(items)} end)

    {:routed, routed}
  end

  @impl true
  def shutdown(_state), do: :ok

  defp find_route(item, routes) do
    case Enum.find(routes, fn {pred, _dest} -> pred.(item) end) do
      {_pred, dest} -> dest
      nil -> :default
    end
  end
end
