defmodule SlitherTest do
  use ExUnit.Case, async: true

  test "dispatch/2 delegates to Slither.Dispatch.run/2" do
    items = Slither.Item.wrap_many([1, 2, 3])

    {:ok, results} =
      Slither.dispatch(items,
        executor: Slither.TestExecutor,
        transform: &(&1 * 2),
        batch_size: 2,
        max_in_flight: 2
      )

    payloads = Enum.map(results, & &1.payload)
    assert payloads == [2, 4, 6]
  end
end
