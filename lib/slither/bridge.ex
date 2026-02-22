defmodule Slither.Bridge do
  @moduledoc """
  Tool registration helpers for exposing Elixir functions to Python
  via SnakeBridge's CallbackRegistry.

  Store views and custom callbacks are registered here, scoped either
  to a session or globally.
  """

  alias Slither.Bridge.ViewRegistry

  @doc """
  Register a view function callable from Python.

  Session-scoped views are tied to a `session_id` and cleaned up
  when the session ends. Global views persist for the application lifetime.
  """
  @spec register_view(Slither.Store.view_spec(), Slither.Context.t()) :: :ok
  def register_view(%{name: name, scope: :session, handler: handler}, %Slither.Context{} = ctx) do
    callback_fn = fn params -> handler.(params, ctx) end
    {:ok, callback_id} = SnakeBridge.CallbackRegistry.register(callback_fn, self())
    ViewRegistry.register(ctx.session_id, name, callback_id)
    :ok
  end

  def register_view(%{name: name, scope: :global, handler: handler}, %Slither.Context{} = ctx) do
    callback_fn = fn params -> handler.(params, ctx) end
    {:ok, callback_id} = SnakeBridge.CallbackRegistry.register(callback_fn, self())
    ViewRegistry.register(:global, name, callback_id)
    :ok
  end

  @doc """
  Unregister all views for a given session.
  """
  @spec unregister_session_views(binary()) :: :ok
  def unregister_session_views(session_id) do
    callback_ids = ViewRegistry.get_session_callbacks(session_id)

    Enum.each(callback_ids, fn {_name, callback_id} ->
      SnakeBridge.CallbackRegistry.unregister(callback_id)
    end)

    ViewRegistry.clear_session(session_id)
    :ok
  end

  @doc """
  List all registered views for a session (or globally).
  """
  @spec list_views(binary() | :global) :: [{atom(), binary()}]
  def list_views(scope) do
    ViewRegistry.get_session_callbacks(scope)
  end
end
