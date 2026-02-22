defmodule Slither.Error do
  @moduledoc """
  Error types for Slither operations.
  """

  defexception [:message, :stage, :reason, :partial_results]

  @type t :: %__MODULE__{
          message: String.t(),
          stage: atom() | nil,
          reason: term(),
          partial_results: map() | nil
        }

  @impl true
  def exception(opts) when is_list(opts) do
    stage = Keyword.get(opts, :stage)
    reason = Keyword.get(opts, :reason)
    partial = Keyword.get(opts, :partial_results)

    message =
      Keyword.get_lazy(opts, :message, fn ->
        base = "Slither error"
        base = if stage, do: "#{base} in stage #{inspect(stage)}", else: base
        if reason, do: "#{base}: #{inspect(reason)}", else: base
      end)

    %__MODULE__{
      message: message,
      stage: stage,
      reason: reason,
      partial_results: partial
    }
  end

  def exception(message) when is_binary(message) do
    %__MODULE__{message: message, stage: nil, reason: nil, partial_results: nil}
  end
end

defmodule Slither.DispatchError do
  @moduledoc """
  Raised when a dispatch batch fails and the error policy is `:halt`.
  """
  defexception [:message, :reason, :batch_ref, :partial_results]

  @impl true
  def exception(opts) do
    reason = Keyword.get(opts, :reason)
    ref = Keyword.get(opts, :batch_ref)
    partial = Keyword.get(opts, :partial_results, [])

    %__MODULE__{
      message: "Dispatch error on batch #{inspect(ref)}: #{inspect(reason)}",
      reason: reason,
      batch_ref: ref,
      partial_results: partial
    }
  end
end

defmodule Slither.PipeError do
  @moduledoc """
  Raised when a pipe run encounters an unrecoverable failure.
  """
  defexception [:message, :pipe, :stage, :reason, :partial_results]

  @impl true
  def exception(opts) do
    pipe = Keyword.get(opts, :pipe)
    stage = Keyword.get(opts, :stage)
    reason = Keyword.get(opts, :reason)
    partial = Keyword.get(opts, :partial_results, %{})

    %__MODULE__{
      message: "Pipe #{inspect(pipe)} failed at stage #{inspect(stage)}: #{inspect(reason)}",
      pipe: pipe,
      stage: stage,
      reason: reason,
      partial_results: partial
    }
  end
end
