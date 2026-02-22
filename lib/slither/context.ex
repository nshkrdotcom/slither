defmodule Slither.Context do
  @moduledoc """
  Run context passed through every stage in a Slither pipeline.

  Contains session affinity for Snakepit/SnakeBridge, store handles,
  run metadata, and telemetry correlation.
  """

  defstruct pipe: nil,
            stage: nil,
            session_id: nil,
            stores: %{},
            metadata: %{},
            runtime: %{}

  @type t :: %__MODULE__{
          pipe: atom() | nil,
          stage: atom() | nil,
          session_id: binary() | nil,
          stores: %{atom() => pid() | atom()},
          metadata: map(),
          runtime: map()
        }

  @doc """
  Create a new context with a fresh session ID.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      pipe: Keyword.get(opts, :pipe),
      stage: Keyword.get(opts, :stage),
      session_id: Keyword.get(opts, :session_id, generate_session_id()),
      stores: Keyword.get(opts, :stores, %{}),
      metadata: Keyword.get(opts, :metadata, %{}),
      runtime: Keyword.get(opts, :runtime, %{})
    }
  end

  @doc """
  Set the current stage name on the context.
  """
  @spec with_stage(t(), atom()) :: t()
  def with_stage(%__MODULE__{} = ctx, stage_name) when is_atom(stage_name) do
    %{ctx | stage: stage_name}
  end

  @doc """
  Merge additional metadata into the context.
  """
  @spec put_metadata(t(), map()) :: t()
  def put_metadata(%__MODULE__{} = ctx, meta) when is_map(meta) do
    %{ctx | metadata: Map.merge(ctx.metadata, meta)}
  end

  defp generate_session_id do
    "slither_#{System.unique_integer([:positive, :monotonic])}_#{System.os_time(:millisecond)}"
  end
end
