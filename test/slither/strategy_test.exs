defmodule Slither.StrategyTest do
  use Supertester.ExUnitFoundation, isolation: :full_isolation

  alias Slither.Dispatch.Strategies.{FixedBatch, KeyPartition, WeightedBatch}
  alias Slither.Item

  describe "FixedBatch" do
    test "splits into fixed-size chunks" do
      items = Item.wrap_many(1..7)
      batches = FixedBatch.batch(items, batch_size: 3)

      assert length(batches) == 3
      assert {1, batch1} = Enum.at(batches, 0)
      assert {2, batch2} = Enum.at(batches, 1)
      assert {3, batch3} = Enum.at(batches, 2)

      assert length(batch1) == 3
      assert length(batch2) == 3
      assert length(batch3) == 1
    end

    test "single batch for small input" do
      items = Item.wrap_many([1, 2])
      batches = FixedBatch.batch(items, batch_size: 10)

      assert length(batches) == 1
      {1, batch} = hd(batches)
      assert length(batch) == 2
    end
  end

  describe "WeightedBatch" do
    test "batches by cumulative weight" do
      items = Item.wrap_many([10, 20, 30, 40, 50])

      batches =
        WeightedBatch.batch(items,
          max_weight: 50,
          weight_fn: fn item -> item.payload end
        )

      # 10+20=30 (fits), +30=60 (exceeds) -> split
      # 30+40=70 (exceeds) -> split
      # 40+50=90 (exceeds) -> split
      assert length(batches) >= 3
    end

    test "single batch when all weights fit" do
      items = Item.wrap_many([1, 2, 3])

      batches =
        WeightedBatch.batch(items,
          max_weight: 100,
          weight_fn: fn item -> item.payload end
        )

      assert length(batches) == 1
    end
  end

  describe "KeyPartition" do
    test "groups by key function" do
      items = Item.wrap_many([:a, :b, :a, :c, :b, :a])

      batches =
        KeyPartition.batch(items,
          key_fn: fn item -> item.payload end
        )

      assert length(batches) == 3

      # Find the :a partition
      {_, a_batch} =
        Enum.find(batches, fn {_ref, group} ->
          Enum.any?(group, &(&1.payload == :a))
        end)

      assert length(a_batch) == 3
    end
  end
end
