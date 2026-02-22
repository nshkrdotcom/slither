defmodule Slither.Dispatch do
  @moduledoc """
  Batched fan-out to Python pools with real backpressure.

  Dispatch takes an enumerable of items, batches them via a pluggable
  strategy, executes each batch through an Executor (Snakepit or
  SnakeBridge), and returns results either preserving input order or
  as-completed.

  Backpressure is enforced via `max_in_flight` — at most that many
  batches are executing concurrently. Upstream enumeration only
  advances when a worker slot becomes available.

  ## Examples

      # Run with SnakeBridge executor
      items = Slither.Item.wrap_many(data)
      ctx = Slither.Context.new(session_id: "my_session")

      {:ok, results} = Slither.Dispatch.run(items,
        executor: Slither.Dispatch.Executors.SnakeBridge,
        module: "my_model",
        function: "predict_batch",
        pool: :gpu_pool,
        batch_size: 32,
        max_in_flight: 4
      )

      # Stream results lazily
      stream = Slither.Dispatch.stream(items,
        executor: Slither.Dispatch.Executors.Snakepit,
        pool: :compute,
        command: "process_batch",
        batch_size: 64
      )
  """

  alias Slither.Dispatch.{Runner, Strategies}

  @type ordering :: :preserve | :as_completed

  @type on_error ::
          :skip
          | :halt
          | {:retry, keyword()}
          | {:route, (Slither.Item.t(), term() -> Slither.Item.t())}

  @type opts :: [
          {:executor, module()}
          | {:strategy, module()}
          | {:batch_size, pos_integer()}
          | {:max_in_flight, pos_integer()}
          | {:ordering, ordering()}
          | {:on_error, on_error()}
          | {:timeout, pos_integer()}
          | {:context, Slither.Context.t()}
          | {atom(), term()}
        ]

  @doc """
  Dispatch items to an executor in batches.

  Returns `{:ok, results}` with results in the same order as input
  (when `ordering: :preserve`), or `{:error, reason, partial_results}`.
  """
  @spec run(Enumerable.t(Slither.Item.t()), opts()) ::
          {:ok, [Slither.Item.t()]} | {:error, term(), [Slither.Item.t()]}
  def run(items, opts) do
    {executor, strategy, ctx, runner_opts} = parse_opts(opts)
    items_list = Enum.to_list(items)
    batches = strategy.batch(items_list, runner_opts)
    Runner.run_batches(batches, executor, ctx, runner_opts)
  end

  @doc """
  Dispatch items returning a lazy stream of results.

  Results emit as batches complete. Useful for large inputs where
  you don't want all results in memory at once.
  """
  @spec stream(Enumerable.t(Slither.Item.t()), opts()) :: Enumerable.t(Slither.Item.t())
  def stream(items, opts) do
    {executor, strategy, ctx, runner_opts} = parse_opts(opts)
    items_list = Enum.to_list(items)
    batches = strategy.batch(items_list, runner_opts)
    Runner.stream_batches(batches, executor, ctx, runner_opts)
  end

  @doc """
  Dispatch a single pre-assembled batch directly.

  No batching strategy is applied. For when you already have
  your batch ready.
  """
  @spec call_batch([Slither.Item.t()], opts()) ::
          {:ok, [Slither.Item.t()]} | {:error, term()}
  def call_batch(batch, opts) do
    {executor, _strategy, ctx, runner_opts} = parse_opts(opts)
    executor.execute_batch(batch, ctx, runner_opts)
  end

  # --- Internal ---

  defp parse_opts(opts) do
    executor = Keyword.fetch!(opts, :executor)

    strategy =
      Keyword.get(opts, :strategy, Strategies.FixedBatch)

    ctx =
      Keyword.get_lazy(opts, :context, fn ->
        Slither.Context.new()
      end)

    runner_opts =
      opts
      |> Keyword.drop([:executor, :strategy, :context])
      |> Keyword.put_new(:batch_size, default_batch_size())
      |> Keyword.put_new(:max_in_flight, default_max_in_flight())
      |> Keyword.put_new(:ordering, default_ordering())
      |> Keyword.put_new(:on_error, default_on_error())
      |> Keyword.put_new(:timeout, 30_000)

    {executor, strategy, ctx, runner_opts}
  end

  defp default_batch_size do
    Application.get_env(:slither, :dispatch, [])
    |> Keyword.get(:default_batch_size, 64)
  end

  defp default_max_in_flight do
    Application.get_env(:slither, :dispatch, [])
    |> Keyword.get(:default_max_in_flight, 8)
  end

  defp default_ordering do
    Application.get_env(:slither, :dispatch, [])
    |> Keyword.get(:default_ordering, :preserve)
  end

  defp default_on_error do
    Application.get_env(:slither, :dispatch, [])
    |> Keyword.get(:default_on_error, :halt)
  end
end
