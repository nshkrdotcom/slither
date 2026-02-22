defmodule Slither.Dispatch.Runner do
  @moduledoc """
  Internal engine that executes batches with bounded concurrency.

  Uses `Task.async_stream` with `max_concurrency` to enforce the
  `max_in_flight` limit, providing real backpressure — upstream
  enumeration only advances when a worker slot is available.
  """

  require Logger

  @type on_error ::
          :skip
          | :halt
          | {:retry, keyword()}
          | {:route, (Slither.Item.t(), term() -> Slither.Item.t())}

  @doc """
  Execute batches with bounded concurrency, returning all results in order.
  """
  @spec run_batches(
          [{pos_integer(), [Slither.Item.t()]}],
          module(),
          Slither.Context.t(),
          keyword()
        ) ::
          {:ok, [Slither.Item.t()]} | {:error, term(), [Slither.Item.t()]}
  def run_batches(batches, executor, ctx, opts) do
    max_in_flight = Keyword.get(opts, :max_in_flight, 8)
    ordering = Keyword.get(opts, :ordering, :preserve)
    on_error = Keyword.get(opts, :on_error, :halt)
    timeout = Keyword.get(opts, :timeout, 30_000)

    results =
      batches
      |> Task.async_stream(
        fn {ref, batch} ->
          execute_with_telemetry(ref, batch, executor, ctx, opts)
        end,
        max_concurrency: max_in_flight,
        timeout: timeout + 5_000,
        ordered: ordering == :preserve
      )
      |> Enum.reduce_while({:ok, []}, fn
        {:ok, {:ok, ref, items}}, {:ok, acc} ->
          {:cont, {:ok, [{ref, items} | acc]}}

        {:ok, {:error, ref, batch, reason}}, {:ok, acc} ->
          handle_batch_error(ref, batch, reason, on_error, acc)

        {:exit, reason}, {:ok, acc} ->
          handle_batch_error(0, [], {:exit, reason}, on_error, acc)
      end)

    case results do
      {:ok, ref_items} ->
        sorted =
          ref_items
          |> Enum.sort_by(fn {ref, _} -> ref end)
          |> Enum.flat_map(fn {_ref, items} -> items end)

        {:ok, sorted}

      {:error, reason, ref_items} ->
        sorted =
          ref_items
          |> Enum.sort_by(fn {ref, _} -> ref end)
          |> Enum.flat_map(fn {_ref, items} -> items end)

        {:error, reason, sorted}
    end
  end

  @doc """
  Execute batches returning a lazy stream of results.
  """
  @spec stream_batches(
          [{pos_integer(), [Slither.Item.t()]}],
          module(),
          Slither.Context.t(),
          keyword()
        ) :: Enumerable.t()
  def stream_batches(batches, executor, ctx, opts) do
    max_in_flight = Keyword.get(opts, :max_in_flight, 8)
    timeout = Keyword.get(opts, :timeout, 30_000)
    on_error = Keyword.get(opts, :on_error, :skip)

    batches
    |> Task.async_stream(
      fn {ref, batch} ->
        execute_with_telemetry(ref, batch, executor, ctx, opts)
      end,
      max_concurrency: max_in_flight,
      timeout: timeout + 5_000,
      ordered: Keyword.get(opts, :ordering, :preserve) == :preserve
    )
    |> Stream.flat_map(fn
      {:ok, {:ok, _ref, items}} ->
        items

      {:ok, {:error, _ref, batch, reason}} ->
        handle_stream_error(batch, reason, on_error)

      {:exit, _reason} ->
        []
    end)
  end

  # --- Internal ---

  defp execute_with_telemetry(ref, batch, executor, ctx, opts) do
    metadata = %{
      batch_ref: ref,
      batch_size: length(batch),
      executor: executor,
      pipe: ctx.pipe,
      stage: ctx.stage
    }

    Slither.Telemetry.span([:slither, :dispatch, :batch], metadata, fn ->
      case executor.execute_batch(batch, ctx, opts) do
        {:ok, results} -> {:ok, ref, results}
        {:error, reason} -> {:error, ref, batch, reason}
      end
    end)
  end

  defp handle_batch_error(ref, batch, reason, on_error, acc) do
    case on_error do
      :skip ->
        skipped =
          Enum.map(batch, fn item ->
            Slither.Item.put_meta(item, %{error: reason, skipped: true})
          end)

        {:cont, {:ok, [{ref, skipped} | acc]}}

      :halt ->
        {:halt, {:error, reason, acc}}

      {:retry, retry_opts} ->
        handle_retry(ref, batch, reason, retry_opts, acc)

      {:route, fun} when is_function(fun, 2) ->
        routed = Enum.map(batch, fn item -> fun.(item, reason) end)
        {:cont, {:ok, [{ref, routed} | acc]}}
    end
  end

  defp handle_retry(_ref, _batch, reason, _retry_opts, acc) do
    # Retry is best handled at the Pipe level where we have access
    # to the full topology. At the Dispatch level, we fall back to halt.
    Logger.warning(
      "[Slither.Dispatch] Retry requested but not supported at dispatch level, halting"
    )

    {:halt, {:error, {:retry_exhausted, reason}, acc}}
  end

  defp handle_stream_error(batch, reason, on_error) do
    case on_error do
      :skip ->
        Enum.map(batch, fn item ->
          Slither.Item.put_meta(item, %{error: reason, skipped: true})
        end)

      :halt ->
        raise Slither.DispatchError,
          reason: reason,
          batch_ref: nil,
          partial_results: []

      {:route, fun} when is_function(fun, 2) ->
        Enum.map(batch, fn item -> fun.(item, reason) end)

      _ ->
        []
    end
  end
end
