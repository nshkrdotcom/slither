defmodule Slither.StoreTest do
  use Supertester.ExUnitFoundation, isolation: :full_isolation

  import Supertester.Assertions, only: [assert_process_alive: 1]
  import Supertester.OTPHelpers, only: [setup_isolated_genserver: 3]

  alias Slither.Store.Server

  setup do
    {:ok, pid} =
      setup_isolated_genserver(Server, "slither_store", init_args: Slither.TestStore)

    assert_process_alive(pid)

    # A synchronous write guarantees handle_continue(:load) has already run.
    :ok = Server.put(pid, :test_data, "__ready__", true)
    :ok = Server.delete(pid, :test_data, "__ready__")

    %{pid: pid}
  end

  test "get/3 reads from ETS directly", %{pid: _pid} do
    assert Server.get(Slither.TestStore, :test_data, "key1") == "value1"
    assert Server.get(Slither.TestStore, :test_data, "key2") == "value2"
    assert Server.get(Slither.TestStore, :test_data, "missing") == nil
  end

  test "get!/3 raises on missing key", %{pid: _pid} do
    assert_raise KeyError, fn ->
      Server.get!(Slither.TestStore, :test_data, "missing")
    end
  end

  test "put/4 writes through GenServer", %{pid: pid} do
    :ok = Server.put(pid, :test_data, "new_key", "new_value")
    assert Server.get(Slither.TestStore, :test_data, "new_key") == "new_value"
  end

  test "update/4 read-modify-write", %{pid: pid} do
    :ok = Server.put(pid, :test_data, "counter", 0)
    {:ok, 1} = Server.update(pid, :test_data, "counter", &(&1 + 1))
    {:ok, 2} = Server.update(pid, :test_data, "counter", &(&1 + 1))
    assert Server.get(Slither.TestStore, :test_data, "counter") == 2
  end

  test "update/4 returns error for missing key", %{pid: pid} do
    assert {:error, :not_found} = Server.update(pid, :test_data, "nope", &(&1 + 1))
  end

  test "delete/3 removes a key", %{pid: pid} do
    :ok = Server.put(pid, :test_data, "temp", "val")
    assert Server.get(Slither.TestStore, :test_data, "temp") == "val"
    :ok = Server.delete(pid, :test_data, "temp")
    assert Server.get(Slither.TestStore, :test_data, "temp") == nil
  end

  test "bulk_insert/3 inserts many records", %{pid: pid} do
    records = for i <- 1..100, do: {"bulk_#{i}", i}
    :ok = Server.bulk_insert(pid, :test_data, records)
    assert Server.get(Slither.TestStore, :test_data, "bulk_50") == 50
  end

  test "size/2 returns table size", %{pid: _pid} do
    assert Server.size(Slither.TestStore, :test_data) >= 3
  end

  test "match/3 finds matching records", %{pid: _pid} do
    results = Server.match(Slither.TestStore, :test_data, {"key1", :_})
    assert results == [{"key1", "value1"}]
  end
end
