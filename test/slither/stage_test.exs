defmodule Slither.StageTest do
  use Supertester.ExUnitFoundation, isolation: :full_isolation

  alias Slither.{Context, Item}
  alias Slither.Stage.{Beam, Python, Router}

  describe "Beam stage" do
    test "handle_item transforms payload" do
      {:ok, state} = Beam.init(handler: fn item, _ctx -> item.payload * 2 end)
      item = Item.wrap(5)
      ctx = Context.new()

      {:ok, [result]} = Beam.handle_item(item, ctx, state)
      assert result.payload == 10
      assert result.id == item.id
    end

    test "handle_item with {:ok, payload} return" do
      {:ok, state} = Beam.init(handler: fn item, _ctx -> {:ok, item.payload + 1} end)
      item = Item.wrap(10)
      ctx = Context.new()

      {:ok, [result]} = Beam.handle_item(item, ctx, state)
      assert result.payload == 11
    end

    test "handle_item with {:skip, reason}" do
      {:ok, state} = Beam.init(handler: fn _item, _ctx -> {:skip, :filtered} end)
      item = Item.wrap("x")
      ctx = Context.new()

      assert {:skip, :filtered} = Beam.handle_item(item, ctx, state)
    end

    test "handle_item with {:route, dest, payload}" do
      {:ok, state} = Beam.init(handler: fn item, _ctx -> {:route, :special, item.payload} end)
      item = Item.wrap("x")
      ctx = Context.new()

      {:route, :special, [routed]} = Beam.handle_item(item, ctx, state)
      assert routed.payload == "x"
    end

    test "cardinality :many expands results" do
      {:ok, state} =
        Beam.init(
          handler: fn item, _ctx -> Enum.map(1..3, &(item.payload * &1)) end,
          cardinality: :many
        )

      item = Item.wrap(10)
      ctx = Context.new()

      {:ok, results} = Beam.handle_item(item, ctx, state)
      assert length(results) == 3
      assert Enum.map(results, & &1.payload) == [10, 20, 30]
    end

    test "handle_batch without batch_handler returns :per_item" do
      {:ok, state} = Beam.init(handler: fn item, _ctx -> item.payload + 1 end)
      items = Item.wrap_many([1, 2, 3])
      ctx = Context.new()

      assert :per_item = Beam.handle_batch(items, ctx, state)
    end

    test "handle_batch with batch_handler processes all items" do
      {:ok, state} =
        Beam.init(
          handler: fn item, _ctx -> item.payload end,
          batch_handler: fn items, _ctx ->
            {:ok, Enum.map(items, fn item -> Item.update_payload(item, item.payload + 1) end)}
          end
        )

      items = Item.wrap_many([1, 2, 3])
      ctx = Context.new()

      {:ok, results} = Beam.handle_batch(items, ctx, state)
      assert Enum.map(results, & &1.payload) == [2, 3, 4]
    end
  end

  describe "Router stage" do
    test "routes items by predicate" do
      {:ok, state} =
        Router.init(
          routes: [
            {fn item -> item.payload > 5 end, :high},
            {fn item -> item.payload > 0 end, :low},
            {fn _ -> true end, :zero}
          ]
        )

      ctx = Context.new()

      {:route, :high, _} = Router.handle_item(Item.wrap(10), ctx, state)
      {:route, :low, _} = Router.handle_item(Item.wrap(3), ctx, state)
      {:route, :zero, _} = Router.handle_item(Item.wrap(0), ctx, state)
    end

    test "handle_batch groups by destination" do
      {:ok, state} =
        Router.init(
          routes: [
            {fn item -> rem(item.payload, 2) == 0 end, :even},
            {fn _ -> true end, :odd}
          ]
        )

      items = Item.wrap_many([1, 2, 3, 4, 5])
      ctx = Context.new()

      {:routed, routed} = Router.handle_batch(items, ctx, state)

      assert length(routed[:even]) == 2
      assert length(routed[:odd]) == 3
      assert Enum.map(routed[:even], & &1.payload) == [2, 4]
      assert Enum.map(routed[:odd], & &1.payload) == [1, 3, 5]
    end

    test "unmatched items route to :default" do
      {:ok, state} =
        Router.init(
          routes: [
            {fn item -> item.payload == :special end, :special}
          ]
        )

      ctx = Context.new()
      {:route, :default, _} = Router.handle_item(Item.wrap(:normal), ctx, state)
    end
  end

  describe "Python stage" do
    test "init defaults to SnakeBridge executor" do
      {:ok, state} = Python.init(module: "test", function: "fn")
      assert state.executor == Slither.Dispatch.Executors.SnakeBridge
    end

    test "handle_batch delegates to Dispatch" do
      {:ok, state} =
        Python.init(
          executor: Slither.TestExecutor,
          transform: &(&1 * 3),
          batch_size: 10
        )

      items = Item.wrap_many([1, 2, 3])
      ctx = Context.new()

      {:ok, results} = Python.handle_batch(items, ctx, state)
      assert Enum.map(results, & &1.payload) == [3, 6, 9]
    end
  end
end
