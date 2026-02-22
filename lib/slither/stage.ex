defmodule Slither.Stage do
  @moduledoc """
  Behaviour for a single processing step in a Slither pipeline.

  Every stage can implement `handle_item/3` (scalar path) and/or
  `handle_batch/3` (vectorized path). The pipe runner chooses the
  most efficient path based on stage capabilities and batch boundaries.

  ## Result Types

    * `{:ok, [items]}` — emit items downstream (1:1 or 1:many)
    * `{:route, destination, [items]}` — route items to a named output
    * `{:routed, %{dest => [items]}}` — batch routing to multiple outputs
    * `{:skip, reason}` — drop the item
    * `{:error, reason}` — error
  """

  @type item_result ::
          {:ok, [Slither.Item.t()]}
          | {:route, atom(), [Slither.Item.t()]}
          | {:skip, term()}
          | {:error, term()}

  @type batch_result ::
          {:ok, [Slither.Item.t()]}
          | {:routed, %{atom() => [Slither.Item.t()]}}
          | {:error, term()}

  @doc """
  Initialize the stage with options. Returns state passed to handlers.
  """
  @callback init(keyword()) :: {:ok, term()} | {:error, term()}

  @doc """
  Process a single item. Return transformed items for downstream.
  """
  @callback handle_item(Slither.Item.t(), Slither.Context.t(), state :: term()) :: item_result()

  @doc """
  Process a batch of items. Optional — defaults to mapping `handle_item/3`.
  """
  @callback handle_batch([Slither.Item.t()], Slither.Context.t(), state :: term()) ::
              batch_result()

  @doc """
  Clean up stage resources.
  """
  @callback shutdown(state :: term()) :: :ok

  @optional_callbacks [init: 1, handle_batch: 3, shutdown: 1]
end
