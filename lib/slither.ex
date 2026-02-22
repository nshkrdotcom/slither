defmodule Slither do
  @moduledoc """
  Low-level BEAM↔Python concurrency substrate.

  Slither provides three reusable primitives on top of SnakeBridge/Snakepit:

  - **Store** — ETS-backed shared state with lock-free concurrent reads
    and serialized writes, exposed to Python via bidirectional tool calls.

  - **Dispatch** — Bounded batched fan-out to Python pools with real
    backpressure, ordering control, and policy-driven error handling.

  - **Pipe** — Stage composition over BEAM + Python steps with routing,
    telemetry, and supervised execution.

  ## Quick Start

      # 1. Define a pipe
      defmodule MyPipe do
        use Slither.Pipe

        pipe :my_pipe do
          stage :transform, :beam, handler: &MyApp.transform/2
          stage :predict, :python,
            module: "model", function: "predict",
            pool: :gpu, batch_size: 32
          output :default
        end
      end

      # 2. Run it
      {:ok, results} = Slither.Pipe.Runner.run(MyPipe, input_data)

  ## Architecture

  Slither adds exactly three things on top of SnakeBridge/Snakepit:

  - **Shared state** on BEAM with Python-visible views
  - **Bounded batching + backpressure**
  - **Stage composition + routing**

  Everything else — Python env management, pool lifecycle, retries,
  circuit breakers, streaming mechanics — is delegated to
  SnakeBridge/Snakepit.
  """

  alias Slither.Pipe

  @doc """
  Convenience for dispatching items to Python in batches.

  See `Slither.Dispatch.run/2` for full options.
  """
  defdelegate dispatch(items, opts), to: Slither.Dispatch, as: :run

  @doc """
  Convenience for running a pipe against input data.

  See `Slither.Pipe.Runner.run/3` for full options.
  """
  def run_pipe(pipe_mod, input, opts \\ []) do
    Pipe.Runner.run(pipe_mod, input, opts)
  end
end
