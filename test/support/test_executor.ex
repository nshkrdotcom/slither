defmodule Slither.TestExecutor do
  @moduledoc """
  In-memory executor for testing. Applies a transform function to each item.
  """

  @behaviour Slither.Dispatch.Executor

  @impl true
  def execute_batch(batch, _ctx, opts) do
    transform = Keyword.get(opts, :transform, fn payload -> payload end)

    case Keyword.get(opts, :simulate_error) do
      nil ->
        results =
          Enum.map(batch, fn item ->
            Slither.Item.update_payload(item, transform.(item.payload))
          end)

        {:ok, results}

      error ->
        {:error, error}
    end
  end
end
