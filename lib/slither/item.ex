defmodule Slither.Item do
  @moduledoc """
  Work envelope carrying a payload through Slither pipelines.

  Every item has a stable ID for ordering and error attribution,
  stage-local metadata for tracing, and a route tag for dispatch
  without coupling payload types.
  """

  @enforce_keys [:id, :payload]
  defstruct id: nil, payload: nil, meta: %{}, route: :default

  @type t :: %__MODULE__{
          id: id(),
          payload: term(),
          meta: map(),
          route: atom()
        }

  @type id :: pos_integer() | binary()

  @doc """
  Wrap a raw payload into an Item with an auto-generated ID.
  """
  @spec wrap(term()) :: t()
  def wrap(payload) do
    %__MODULE__{id: System.unique_integer([:positive, :monotonic]), payload: payload}
  end

  @doc """
  Wrap a list of payloads into Items with sequential IDs.
  """
  @spec wrap_many(Enumerable.t()) :: [t()]
  def wrap_many(payloads) do
    Enum.map(payloads, &wrap/1)
  end

  @doc """
  Update the payload of an item, preserving ID and metadata.
  """
  @spec update_payload(t(), term()) :: t()
  def update_payload(%__MODULE__{} = item, new_payload) do
    %{item | payload: new_payload}
  end

  @doc """
  Merge metadata into an item's meta map.
  """
  @spec put_meta(t(), map()) :: t()
  def put_meta(%__MODULE__{} = item, new_meta) when is_map(new_meta) do
    %{item | meta: Map.merge(item.meta, new_meta)}
  end
end
