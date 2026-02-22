defmodule Slither.Pipe do
  @moduledoc """
  Compose BEAM and Python stages into a supervised pipeline.

  A Pipe is a sequence of stages with routing, error handling,
  and session-scoped tool registration. Define pipes using the
  DSL and execute them with `Slither.Pipe.Runner`.

  ## Example

      defmodule MyPipe do
        use Slither.Pipe

        pipe :my_pipe do
          store :features, MyApp.FeatureStore

          stage :enrich, :beam,
            handler: &MyApp.enrich/2

          stage :score, :python,
            executor: Slither.Dispatch.Executors.SnakeBridge,
            module: "my_model",
            function: "predict_batch",
            pool: :gpu_pool,
            batch_size: 32

          stage :route, :router,
            routes: [
              {&MyApp.high_confidence?/1, :accept},
              {&MyApp.low_confidence?/1, :review},
              {fn _ -> true end, :reject}
            ]

          output :accept
          output :review
          output :reject

          on_error :score, :skip
          on_error :*, :halt
        end
      end

      # Run the pipe
      {:ok, results} = Slither.Pipe.Runner.run(MyPipe, input_data)
      # => %{accept: [...], review: [...], reject: [...], default: [...]}
  """

  defmacro __using__(opts) do
    quote do
      use Slither.Pipe.Builder, unquote(opts)
    end
  end
end
