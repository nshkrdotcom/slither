defmodule Slither.ItemTest do
  use ExUnit.Case, async: true

  alias Slither.Item

  test "wrap/1 creates an item with auto ID" do
    item = Item.wrap("hello")
    assert item.payload == "hello"
    assert is_integer(item.id)
    assert item.meta == %{}
    assert item.route == :default
  end

  test "wrap_many/1 wraps a list of payloads" do
    items = Item.wrap_many([1, 2, 3])
    assert length(items) == 3
    assert Enum.map(items, & &1.payload) == [1, 2, 3]

    # IDs are unique and monotonic
    ids = Enum.map(items, & &1.id)
    assert ids == Enum.sort(ids)
    assert length(Enum.uniq(ids)) == 3
  end

  test "update_payload/2 preserves ID and meta" do
    item = Item.wrap("old") |> Item.put_meta(%{stage: :test})
    updated = Item.update_payload(item, "new")

    assert updated.id == item.id
    assert updated.payload == "new"
    assert updated.meta == %{stage: :test}
  end

  test "put_meta/2 merges metadata" do
    item = Item.wrap("x") |> Item.put_meta(%{a: 1})
    item = Item.put_meta(item, %{b: 2})

    assert item.meta == %{a: 1, b: 2}
  end
end
