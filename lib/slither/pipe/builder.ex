defmodule Slither.Pipe.Builder do
  @moduledoc """
  DSL macros for defining Slither pipes.

  ## Example

      defmodule MyPipe do
        use Slither.Pipe

        pipe :my_pipe do
          store :feature_store, MyApp.FeatureStore

          stage :enrich, :beam, handler: &MyApp.enrich/2
          stage :score, :python,
            executor: Slither.Dispatch.Executors.Snakepit,
            pool: :compute,
            command: "score_batch",
            batch_size: 64

          stage :route, :router,
            routes: [
              {&MyApp.good?/1, :ok},
              {&MyApp.needs_review?/1, :review},
              {fn _ -> true end, :reject}
            ]

          output :ok
          output :review
          output :reject

          on_error :score, {:retry, max_attempts: 3}
          on_error :*, :skip
        end
      end
  """

  defmacro __using__(_opts) do
    quote do
      import Slither.Pipe.Builder

      Module.register_attribute(__MODULE__, :slither_pipe_def, accumulate: false)
    end
  end

  defmacro pipe(name, do: block) do
    quote do
      @slither_pipe_def true

      def __pipe_definition__ do
        var!(slither_stages) = []
        var!(slither_stores) = %{}
        var!(slither_outputs) = [:default]
        var!(slither_errors) = %{}

        unquote(block)

        %Slither.Pipe.Definition{
          name: unquote(name),
          stages: Enum.reverse(var!(slither_stages)),
          stores: var!(slither_stores),
          outputs: MapSet.new(var!(slither_outputs)),
          error_policy: var!(slither_errors)
        }
      end
    end
  end

  defmacro stage(name, type, opts \\ []) do
    quote do
      var!(slither_stages) = [
        %{name: unquote(name), type: unquote(type), opts: unquote(opts)}
        | var!(slither_stages)
      ]
    end
  end

  defmacro store(name, module) do
    quote do
      var!(slither_stores) = Map.put(var!(slither_stores), unquote(name), unquote(module))
    end
  end

  defmacro output(name) do
    quote do
      var!(slither_outputs) = [unquote(name) | var!(slither_outputs)]
    end
  end

  defmacro on_error(stage_name, policy) do
    quote do
      var!(slither_errors) = Map.put(var!(slither_errors), unquote(stage_name), unquote(policy))
    end
  end
end
