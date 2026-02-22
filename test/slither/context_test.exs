defmodule Slither.ContextTest do
  use ExUnit.Case, async: true

  alias Slither.Context

  test "new/0 generates a session ID" do
    ctx = Context.new()
    assert is_binary(ctx.session_id)
    assert String.starts_with?(ctx.session_id, "slither_")
  end

  test "new/1 accepts explicit session ID" do
    ctx = Context.new(session_id: "my_session")
    assert ctx.session_id == "my_session"
  end

  test "with_stage/2 sets the stage name" do
    ctx = Context.new() |> Context.with_stage(:scoring)
    assert ctx.stage == :scoring
  end

  test "put_metadata/2 merges metadata" do
    ctx = Context.new(metadata: %{a: 1}) |> Context.put_metadata(%{b: 2})
    assert ctx.metadata == %{a: 1, b: 2}
  end
end
