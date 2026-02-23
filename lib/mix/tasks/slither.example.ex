defmodule Mix.Tasks.Slither.Example do
  @shortdoc "Run Slither examples"
  @moduledoc """
  Run Slither examples showing why process isolation beats free-threaded Python.

  Each example is a real stress test run at the configured Snakepit pool size
  (48 workers by default). The Python baseline runs in free-threaded Python 3.14
  and executes both:
    * `unsafe_threaded` (naive shared-state threading)
    * `safe_python` (synchronized pure-Python design)

  ## Usage

      mix slither.example                  # List available examples
      mix slither.example text_analysis    # Run a specific example
      mix slither.example --all            # Run all stdlib examples (1-3)
      mix slither.example --no-baseline    # Skip pure-Python baseline comparison
      mix slither.baseline text_analysis   # Run baseline only (no Slither pipeline)

  ## Available Examples

      text_analysis   5,000 docs, shared accumulator races         [stdlib]
      batch_stats     2,000 datasets, fault-isolated Welford       [stdlib]
      data_etl        15,000 rows, hot-reload under contention     [stdlib]
      ml_scoring      2,000 records, session-scoped isolation      [scikit-learn]
      image_pipeline  200 images, backpressure + memory safety     [Pillow]

  Examples marked [stdlib] require no Python packages beyond the standard library.
  Examples with third-party deps are installed automatically via Snakepit/uv.
  """

  use Mix.Task

  alias Slither.Examples.Baseline
  alias Slither.Examples.Reporter

  @examples %{
    "text_analysis" =>
      {Slither.Examples.TextAnalysis.TextPipe, :run_demo, "5K Docs / Shared Accumulator Races",
       :stdlib},
    "batch_stats" =>
      {Slither.Examples.BatchStats.StatsDemo, :run_demo,
       "2K Datasets / Fault-Isolated Parallel Compute", :stdlib},
    "data_etl" =>
      {Slither.Examples.DataEtl.EtlPipe, :run_demo, "15K Rows / Hot-Reload Under Contention",
       :stdlib},
    "ml_scoring" =>
      {Slither.Examples.MlScoring.ScoringPipe, :run_demo,
       "2K Records / Session-Scoped State Isolation", :sklearn},
    "image_pipeline" =>
      {Slither.Examples.ImagePipeline.ThumbnailDemo, :run_demo,
       "200 Images / Backpressure + Memory Safety", :pillow}
  }

  # PEP-440 requirements for non-stdlib examples, installed via Snakepit/uv
  @python_deps %{
    sklearn: ["scikit-learn~=1.3", "numpy~=1.26"],
    pillow: ["Pillow~=10.0"]
  }
  @default_workers 48

  @impl Mix.Task
  def run(args) do
    # CRITICAL: Set PYTHONPATH before starting the app so Python workers can import our example modules
    examples_path = Path.join(File.cwd!(), "priv/python/examples")
    existing = System.get_env("PYTHONPATH", "")

    new_path =
      if existing == "",
        do: examples_path,
        else: "#{examples_path}:#{existing}"

    System.put_env("PYTHONPATH", new_path)

    # Now start the application (which starts Snakepit Python workers)
    Mix.Task.run("app.start")

    # Wait for the Snakepit worker pool to be ready.
    # Large pools with batch startup may take several seconds.
    Snakepit.Pool.await_ready()

    # Print system info header
    Reporter.print_system_info()

    case parse_args(args) do
      :list ->
        list_examples()

      {:run, name, baseline?} ->
        run_example(name, baseline?)

      {:all, baseline?} ->
        run_all_stdlib(baseline?)

      {:error, msg} ->
        Mix.shell().error(msg)
        list_examples()
    end
  end

  defp parse_args(args) do
    {opts, positional, invalid} =
      OptionParser.parse(args, strict: [all: :boolean, no_baseline: :boolean])

    baseline? = not Keyword.get(opts, :no_baseline, false)
    all? = Keyword.get(opts, :all, false)

    case invalid do
      [] -> parse_example_selection(all?, positional, baseline?)
      _ -> {:error, "Unknown options: #{inspect(invalid)}"}
    end
  end

  defp parse_example_selection(true, [], baseline?), do: {:all, baseline?}

  defp parse_example_selection(true, _positional, _baseline?) do
    {:error, "Cannot combine --all with an example name"}
  end

  defp parse_example_selection(false, [], _baseline?), do: :list

  defp parse_example_selection(false, [name], baseline?),
    do: parse_single_example(name, baseline?)

  defp parse_example_selection(false, _positional, _baseline?) do
    {:error, "Usage: mix slither.example [name|--all] [--no-baseline]"}
  end

  defp parse_single_example(name, baseline?) do
    if Map.has_key?(@examples, name) do
      {:run, name, baseline?}
    else
      {:error, "Unknown example: #{name}"}
    end
  end

  defp list_examples do
    Mix.shell().info("\nAvailable Slither examples (#{worker_count()} workers, stress-scale):\n")

    for {name, {_mod, _fun, desc, deps}} <- Enum.sort(@examples) do
      dep_tag = if deps == :stdlib, do: "[stdlib]", else: "[#{deps}]"
      Mix.shell().info("  #{String.pad_trailing(name, 18)} #{desc} #{dep_tag}")
    end

    Mix.shell().info("\nRun with: mix slither.example <name>")
    Mix.shell().info("Run all stdlib examples: mix slither.example --all")
    Mix.shell().info("Skip baseline comparison: mix slither.example --no-baseline\n")
    Mix.shell().info("Run baseline only: mix slither.baseline <name>\n")
  end

  defp run_example(name, baseline?) do
    {mod, fun, desc, deps} = @examples[name]

    Mix.shell().info("\n\u25b8 Running: #{desc}")
    Mix.shell().info("  Module: #{inspect(mod)}\n")

    ensure_python_deps(deps)

    try do
      apply(mod, fun, [])
      maybe_run_baseline(name, baseline?)
    rescue
      e ->
        Mix.shell().error("\nExample failed: #{Exception.message(e)}")
        Mix.shell().error(Exception.format(:error, e, __STACKTRACE__))
    end
  end

  defp run_all_stdlib(baseline?) do
    stdlib_examples =
      @examples
      |> Enum.filter(fn {_name, {_mod, _fun, _desc, deps}} -> deps == :stdlib end)
      |> Enum.sort_by(fn {name, _} -> name end)

    Mix.shell().info(
      "\nRunning #{length(stdlib_examples)} stdlib examples (#{worker_count()} workers)...\n"
    )

    for {name, _} <- stdlib_examples do
      run_example(name, baseline?)
      Mix.shell().info("\n" <> String.duplicate("\u2500", 60) <> "\n")
    end

    Mix.shell().info("All stdlib examples complete!")
  end

  defp maybe_run_baseline(_name, false), do: :ok

  defp maybe_run_baseline(name, true) do
    Mix.shell().info(
      "\n\u25b8 Running: Pure-Python Baseline (free-threaded, unsafe+safe, #{worker_count()} threads)"
    )

    case Baseline.run(name) do
      {:ok, output} ->
        Mix.shell().info(output)

      {:error, reason, output} ->
        Mix.shell().error("  Baseline error: #{inspect(reason)}")

        if output != "" do
          Mix.shell().error(output)
        end
    end
  end

  defp ensure_python_deps(:stdlib), do: :ok

  defp ensure_python_deps(dep_key) do
    case Map.fetch(@python_deps, dep_key) do
      {:ok, requirements} ->
        Mix.shell().info(
          "Ensuring Python packages via Snakepit/uv: #{Enum.join(requirements, ", ")}"
        )

        Snakepit.PythonPackages.ensure!({:list, requirements}, quiet: true)
        Mix.shell().info("  Packages ready.\n")

      :error ->
        :ok
    end
  end

  defp worker_count do
    case Application.get_env(:snakepit, :pool_config) do
      %{pool_size: size} when is_integer(size) and size > 0 ->
        size

      _ ->
        Application.get_env(:snakepit, :pool_size, @default_workers)
    end
  end
end
