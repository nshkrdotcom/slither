defmodule Slither.Dispatch.Executors.Snakepit do
  @moduledoc """
  Executor that sends batches via Snakepit tool/adapter commands.

  Use when the Python work is exposed as a Snakepit tool command.

  ## Options

    * `:pool` - Snakepit pool name (required)
    * `:command` - Python command/tool name (required)
    * `:timeout` - Call timeout in milliseconds (default: 30_000)
  """

  @behaviour Slither.Dispatch.Executor

  @impl true
  def execute_batch(batch, ctx, opts) do
    pool = Keyword.fetch!(opts, :pool)
    command = Keyword.fetch!(opts, :command)
    timeout = Keyword.get(opts, :timeout, 30_000)

    payload = %{"items" => Enum.map(batch, & &1.payload)}

    runtime_opts = [
      pool_name: pool,
      timeout: timeout
    ]

    runtime_opts =
      if ctx.session_id do
        Keyword.put(runtime_opts, :session_id, ctx.session_id)
      else
        runtime_opts
      end

    case Snakepit.execute(command, payload, runtime_opts) do
      {:ok, %{"items" => results}} when is_list(results) ->
        updated =
          Enum.zip_with(batch, results, fn item, result_payload ->
            Slither.Item.update_payload(item, result_payload)
          end)

        {:ok, updated}

      {:ok, other} ->
        {:error, {:unexpected_response, other}}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
