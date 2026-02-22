defmodule Slither.Stage.Python do
  @moduledoc """
  Stage that dispatches batches to Python via `Slither.Dispatch`.

  Items are automatically batched and sent through the configured
  executor. This stage always operates in batch mode.

  ## Options

    * `:executor` - Executor module (default: `Slither.Dispatch.Executors.SnakeBridge`)
    * `:module` - Python module path (for SnakeBridge executor)
    * `:function` - Python function name (for SnakeBridge executor)
    * `:command` - Python command name (for Snakepit executor)
    * `:pool` - Snakepit pool name
    * `:batch_size` - Items per batch (default: 64)
    * `:max_in_flight` - Max concurrent batches (default: 8)
    * `:timeout` - Per-batch timeout in ms (default: 30_000)
    * `:on_error` - Error policy for failed batches
  """

  @behaviour Slither.Stage

  @impl true
  def init(opts) do
    executor = Keyword.get(opts, :executor, Slither.Dispatch.Executors.SnakeBridge)

    {:ok,
     %{
       executor: executor,
       dispatch_opts: Keyword.put(opts, :executor, executor)
     }}
  end

  @impl true
  def handle_item(item, ctx, state) do
    # Single items still go through dispatch for consistency
    case Slither.Dispatch.call_batch([item], Keyword.put(state.dispatch_opts, :context, ctx)) do
      {:ok, results} -> {:ok, results}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def handle_batch(items, ctx, state) do
    dispatch_opts = Keyword.put(state.dispatch_opts, :context, ctx)

    case Slither.Dispatch.run(items, dispatch_opts) do
      {:ok, results} -> {:ok, results}
      {:error, reason, _partial} -> {:error, reason}
    end
  end

  @impl true
  def shutdown(_state), do: :ok
end
