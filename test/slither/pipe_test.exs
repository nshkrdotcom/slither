defmodule Slither.PipeTest do
  use Supertester.ExUnitFoundation, isolation: :full_isolation

  alias Slither.Pipe.Runner

  defmodule SimplePipe do
    use Slither.Pipe

    pipe :simple do
      stage(:double, :beam, handler: fn item, _ctx -> item.payload * 2 end)
      stage(:add_ten, :beam, handler: fn item, _ctx -> item.payload + 10 end)
      output(:default)
    end
  end

  defmodule RoutingPipe do
    use Slither.Pipe

    pipe :routing do
      stage(:classify, :router,
        routes: [
          {fn item -> item.payload >= 5 end, :high},
          {fn _ -> true end, :low}
        ]
      )

      output(:high)
      output(:low)
    end
  end

  defmodule MixedPipe do
    use Slither.Pipe

    pipe :mixed do
      stage(:transform, :beam, handler: fn item, _ctx -> item.payload * 3 end)

      stage(:python_sim, :python,
        executor: Slither.TestExecutor,
        transform: &(&1 + 100),
        batch_size: 5
      )

      output(:default)
    end
  end

  defmodule SkipErrorPipe do
    use Slither.Pipe

    pipe :skip_errors do
      stage(:maybe_fail, :beam,
        handler: fn item, _ctx ->
          if item.payload == :fail, do: {:error, :bad_value}, else: item.payload
        end
      )

      on_error(:maybe_fail, :skip)
      output(:default)
    end
  end

  describe "SimplePipe" do
    test "chains beam stages sequentially" do
      {:ok, outputs} = Runner.run(SimplePipe, [1, 2, 3])

      payloads = Enum.map(outputs[:default], & &1.payload)
      # (1*2)+10=12, (2*2)+10=14, (3*2)+10=16
      assert payloads == [12, 14, 16]
    end
  end

  describe "RoutingPipe" do
    test "routes items to named outputs" do
      {:ok, outputs} = Runner.run(RoutingPipe, [1, 3, 5, 7, 2, 8])

      high_payloads = Enum.map(outputs[:high], & &1.payload)
      low_payloads = Enum.map(outputs[:low], & &1.payload)

      assert high_payloads == [5, 7, 8]
      assert low_payloads == [1, 3, 2]
    end
  end

  describe "MixedPipe" do
    test "chains beam and python stages" do
      {:ok, outputs} = Runner.run(MixedPipe, [1, 2, 3])

      payloads = Enum.map(outputs[:default], & &1.payload)
      # (1*3)+100=103, (2*3)+100=106, (3*3)+100=109
      assert payloads == [103, 106, 109]
    end
  end

  describe "SkipErrorPipe" do
    test "skips items that error" do
      {:ok, outputs} = Runner.run(SkipErrorPipe, [:ok, :fail, :also_ok])

      payloads = Enum.map(outputs[:default], & &1.payload)
      assert payloads == [:ok, :also_ok]
    end
  end

  describe "definition" do
    test "pipe definition is accessible" do
      def_ = SimplePipe.__pipe_definition__()
      assert def_.name == :simple
      assert length(def_.stages) == 2
      assert MapSet.member?(def_.outputs, :default)
    end

    test "routing pipe declares outputs" do
      def_ = RoutingPipe.__pipe_definition__()
      assert MapSet.member?(def_.outputs, :high)
      assert MapSet.member?(def_.outputs, :low)
    end
  end

  describe "empty input" do
    test "returns empty outputs" do
      {:ok, outputs} = Runner.run(SimplePipe, [])
      assert outputs[:default] == []
    end
  end
end
