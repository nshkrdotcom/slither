defmodule Slither.DispatchTest do
  use ExUnit.Case, async: true

  alias Slither.{Dispatch, Item}

  describe "run/2" do
    test "dispatches items in batches and returns results in order" do
      items = Item.wrap_many(1..10)

      {:ok, results} =
        Dispatch.run(items,
          executor: Slither.TestExecutor,
          transform: &(&1 * 10),
          batch_size: 3,
          max_in_flight: 2
        )

      payloads = Enum.map(results, & &1.payload)
      assert payloads == [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    end

    test "handles empty input" do
      {:ok, results} =
        Dispatch.run([],
          executor: Slither.TestExecutor,
          batch_size: 10
        )

      assert results == []
    end

    test "single batch when items < batch_size" do
      items = Item.wrap_many([1, 2])

      {:ok, results} =
        Dispatch.run(items,
          executor: Slither.TestExecutor,
          transform: &(&1 + 100),
          batch_size: 10
        )

      payloads = Enum.map(results, & &1.payload)
      assert payloads == [101, 102]
    end

    test "on_error: :halt returns error with partial results" do
      items = Item.wrap_many([1, 2, 3])

      {:error, :boom, _partial} =
        Dispatch.run(items,
          executor: Slither.TestExecutor,
          simulate_error: :boom,
          batch_size: 1,
          max_in_flight: 1,
          on_error: :halt
        )
    end

    test "on_error: :skip continues past errors" do
      items = Item.wrap_many([1, 2, 3])

      {:ok, results} =
        Dispatch.run(items,
          executor: Slither.TestExecutor,
          simulate_error: :boom,
          batch_size: 1,
          max_in_flight: 1,
          on_error: :skip
        )

      # All items should have error metadata
      assert Enum.all?(results, fn item -> item.meta[:skipped] == true end)
    end
  end

  describe "call_batch/2" do
    test "sends a single pre-assembled batch" do
      batch = Item.wrap_many([10, 20, 30])

      {:ok, results} =
        Dispatch.call_batch(batch,
          executor: Slither.TestExecutor,
          transform: &(&1 * 2)
        )

      payloads = Enum.map(results, & &1.payload)
      assert payloads == [20, 40, 60]
    end
  end

  describe "stream/2" do
    test "returns a lazy stream of results" do
      items = Item.wrap_many(1..5)

      stream =
        Dispatch.stream(items,
          executor: Slither.TestExecutor,
          transform: &to_string/1,
          batch_size: 2
        )

      results = Enum.to_list(stream)
      payloads = Enum.map(results, & &1.payload)
      assert payloads == ["1", "2", "3", "4", "5"]
    end
  end
end
