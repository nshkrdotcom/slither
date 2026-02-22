defmodule Slither.Dispatch.Executors.SnakeBridge do
  @moduledoc """
  Executor that sends batches via SnakeBridge universal FFI calls.

  Use when the Python function is a module/function pair.

  ## Options

    * `:module` - Python module path (required)
    * `:function` - Python function name (required)
    * `:pool` - Snakepit pool name (optional)
    * `:timeout` - Call timeout in milliseconds (default: 30_000)
  """

  @behaviour Slither.Dispatch.Executor

  @impl true
  def execute_batch(batch, ctx, opts) do
    module = Keyword.fetch!(opts, :module)
    function = Keyword.fetch!(opts, :function)
    pool = Keyword.get(opts, :pool)
    timeout = Keyword.get(opts, :timeout, 30_000)

    payloads = Enum.map(batch, & &1.payload)

    runtime_opts =
      [timeout: timeout]
      |> maybe_put(:pool_name, pool)
      |> maybe_put(:session_id, ctx.session_id)

    case SnakeBridge.call(module, function, [payloads], __runtime__: runtime_opts) do
      {:ok, results} when is_list(results) ->
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

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
end
