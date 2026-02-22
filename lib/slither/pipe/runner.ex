defmodule Slither.Pipe.Runner do
  @moduledoc """
  Executes a pipe definition against input data.

  The runner:
  1. Creates or uses a session ID for Snakepit/SnakeBridge affinity
  2. Ensures store processes exist and registers views as session tools
  3. Executes stages sequentially, routing items between stages
  4. Collects results into output buckets
  5. Cleans up session tools on completion
  """

  require Logger

  alias Slither.{Bridge, Context, Item, Pipe.Definition, Telemetry}

  @doc """
  Execute a pipe against input, returning results grouped by output.

  Input can be raw payloads (auto-wrapped into Items) or pre-wrapped Items.

  ## Options

    * `:session_id` - Session ID for Snakepit affinity (auto-generated if omitted)
    * `:metadata` - Additional metadata for the run context
  """
  @spec run(module(), Enumerable.t(), keyword()) ::
          {:ok, %{atom() => [Item.t()]}}
          | {:error, term(), %{atom() => [Item.t()]}}
  def run(pipe_mod, input, opts \\ []) do
    pipe_def = pipe_mod.__pipe_definition__()

    ctx = build_context(pipe_def, opts)
    items = wrap_input(input)

    Telemetry.span(
      [:slither, :pipe, :run],
      %{pipe: pipe_def.name, input_count: length(items)},
      fn ->
        try do
          register_views(pipe_def, ctx)
          result = execute_stages(pipe_def, items, ctx)
          Bridge.unregister_session_views(ctx.session_id)
          result
        rescue
          e ->
            Bridge.unregister_session_views(ctx.session_id)
            reraise e, __STACKTRACE__
        end
      end
    )
  end

  @doc """
  Execute a pipe returning a stream per output.

  Note: stages still execute eagerly per-batch within the stream,
  but the overall pipeline is lazy — batches flow through as demand allows.
  """
  @spec stream(module(), Enumerable.t(), keyword()) ::
          %{atom() => Enumerable.t(Item.t())}
  def stream(pipe_mod, input, opts \\ []) do
    # For v0.1, stream delegates to run and wraps results as streams
    case run(pipe_mod, input, opts) do
      {:ok, outputs} ->
        Map.new(outputs, fn {name, items} -> {name, Stream.map(items, & &1)} end)

      {:error, _reason, outputs} ->
        Map.new(outputs, fn {name, items} -> {name, Stream.map(items, & &1)} end)
    end
  end

  # --- Internal ---

  defp build_context(pipe_def, opts) do
    Context.new(
      pipe: pipe_def.name,
      session_id: Keyword.get(opts, :session_id),
      stores: pipe_def.stores,
      metadata: Keyword.get(opts, :metadata, %{})
    )
  end

  defp wrap_input(input) do
    input
    |> Enum.to_list()
    |> Enum.map(fn
      %Item{} = item -> item
      payload -> Item.wrap(payload)
    end)
  end

  defp register_views(pipe_def, ctx) do
    for {_name, store_mod} <- pipe_def.stores,
        function_exported?(store_mod, :views, 0),
        view_spec <- store_mod.views() do
      Bridge.register_view(view_spec, ctx)
    end

    :ok
  end

  defp execute_stages(pipe_def, items, ctx) do
    initial_outputs = init_outputs(pipe_def)

    pipe_def.stages
    |> Enum.reduce_while({:flowing, items}, &reduce_stage(&1, &2, pipe_def, ctx))
    |> collect_results(initial_outputs)
  end

  defp reduce_stage(_stage_entry, {:flowing, []}, _pipe_def, _ctx) do
    {:halt, {:flowing, []}}
  end

  defp reduce_stage(stage_entry, {:flowing, current_items}, pipe_def, ctx) do
    stage_ctx = Context.with_stage(ctx, stage_entry.name)
    error_policy = Definition.error_policy_for(pipe_def, stage_entry.name)
    metadata = %{pipe: pipe_def.name, stage: stage_entry.name, type: stage_entry.type}

    Telemetry.span([:slither, :pipe, :stage], metadata, fn ->
      case execute_stage(stage_entry, current_items, stage_ctx, error_policy) do
        {:ok, out_items} -> {:cont, {:flowing, out_items}}
        {:routed, routed_map} -> {:halt, {:routed, routed_map}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp collect_results({:flowing, final_items}, initial_outputs) do
    {:ok, Map.put(initial_outputs, :default, final_items)}
  end

  defp collect_results({:routed, routed_map}, initial_outputs) do
    outputs =
      Enum.reduce(routed_map, initial_outputs, fn {dest, items}, acc ->
        Map.update(acc, dest, items, &(&1 ++ items))
      end)

    {:ok, outputs}
  end

  defp collect_results({:error, reason}, initial_outputs) do
    {:error, reason, initial_outputs}
  end

  defp init_outputs(pipe_def) do
    pipe_def.outputs
    |> MapSet.to_list()
    |> Map.new(fn name -> {name, []} end)
  end

  defp execute_stage(stage_entry, items, ctx, error_policy) do
    stage_mod = Definition.stage_module(stage_entry.type)
    {:ok, state} = stage_mod.init(stage_entry.opts)

    try do
      run_stage_batch(stage_mod, items, ctx, state, error_policy)
    after
      if function_exported?(stage_mod, :shutdown, 1) do
        stage_mod.shutdown(state)
      end
    end
  end

  defp run_stage_batch(stage_mod, items, ctx, state, error_policy) do
    batch_result =
      if function_exported?(stage_mod, :handle_batch, 3) do
        stage_mod.handle_batch(items, ctx, state)
      else
        :per_item
      end

    case batch_result do
      {:ok, results} -> {:ok, results}
      {:routed, routed} -> {:routed, routed}
      {:error, reason} -> handle_stage_error(reason, error_policy)
      :per_item -> run_per_item(stage_mod, items, ctx, state, error_policy)
    end
  end

  defp run_per_item(stage_mod, items, ctx, state, error_policy) do
    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      item
      |> stage_mod.handle_item(ctx, state)
      |> apply_item_result(acc, error_policy)
    end)
  end

  defp apply_item_result({:ok, out_items}, acc, _policy) do
    {:cont, {:ok, acc ++ out_items}}
  end

  defp apply_item_result({:route, dest, out_items}, acc, _policy) do
    {:cont, {:ok, acc ++ Enum.map(out_items, &%{&1 | route: dest})}}
  end

  defp apply_item_result({:skip, _reason}, acc, _policy) do
    {:cont, {:ok, acc}}
  end

  defp apply_item_result({:error, _reason}, acc, :skip) do
    {:cont, {:ok, acc}}
  end

  defp apply_item_result({:error, reason}, _acc, :halt) do
    {:halt, {:error, reason}}
  end

  defp apply_item_result({:error, _reason}, acc, _policy) do
    {:cont, {:ok, acc}}
  end

  defp handle_stage_error(_reason, :skip), do: {:ok, []}
  defp handle_stage_error(reason, :halt), do: {:error, reason}

  defp handle_stage_error(reason, {:retry, _opts}) do
    Logger.warning("[Slither.Pipe] Retry not yet implemented at pipe level")
    {:error, reason}
  end

  defp handle_stage_error(_reason, {:route, _fun}), do: {:ok, []}
end
