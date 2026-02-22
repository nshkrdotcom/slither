defmodule Slither.Telemetry do
  @moduledoc """
  Telemetry event definitions and span helpers for Slither.

  All events are emitted under the `[:slither, ...]` prefix.

  ## Dispatch Events

    * `[:slither, :dispatch, :batch, :start]` - Batch execution started
    * `[:slither, :dispatch, :batch, :stop]` - Batch execution completed
    * `[:slither, :dispatch, :batch, :exception]` - Batch execution failed

  ## Store Events

    * `[:slither, :store, :write]` - Store write operation
    * `[:slither, :store, :reload, :start]` - Store reload started
    * `[:slither, :store, :reload, :stop]` - Store reload completed

  ## Pipe Events

    * `[:slither, :pipe, :run, :start]` - Pipe run started
    * `[:slither, :pipe, :run, :stop]` - Pipe run completed
    * `[:slither, :pipe, :run, :exception]` - Pipe run failed
    * `[:slither, :pipe, :stage, :start]` - Stage within pipe started
    * `[:slither, :pipe, :stage, :stop]` - Stage within pipe completed

  ## Bridge Events

    * `[:slither, :bridge, :view, :start]` - View callback started
    * `[:slither, :bridge, :view, :stop]` - View callback completed
  """

  @doc """
  Execute a function within a telemetry span.

  Emits `:start`, `:stop`, and `:exception` events automatically.
  """
  @spec span(list(atom()), map(), (-> term())) :: term()
  def span(event_prefix, metadata, fun) when is_list(event_prefix) and is_function(fun, 0) do
    :telemetry.span(event_prefix, metadata, fn ->
      result = fun.()
      {result, metadata}
    end)
  end

  @doc """
  Emit a single telemetry event.
  """
  @spec emit(list(atom()), map(), map()) :: :ok
  def emit(event, measurements \\ %{}, metadata \\ %{}) do
    :telemetry.execute(event, measurements, metadata)
  end
end
