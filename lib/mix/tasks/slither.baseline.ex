defmodule Mix.Tasks.Slither.Baseline do
  @shortdoc "Run pure-Python baselines only"
  @moduledoc """
  Run pure-Python baseline comparisons without running Slither example pipelines.

  ## Usage

      mix slither.baseline                 # List available baseline targets
      mix slither.baseline text_analysis   # Run one baseline target
      mix slither.baseline --all           # Run all baseline targets
  """

  use Mix.Task

  alias Slither.Examples.Baseline

  @targets ~w(text_analysis batch_stats data_etl ml_scoring image_pipeline)

  @impl Mix.Task
  def run(args) do
    case parse_args(args) do
      :list ->
        list_targets()

      {:run, name} ->
        case run_target(name) do
          :ok ->
            :ok

          {:error, _reason} ->
            Mix.raise("Baseline failed for #{name}")
        end

      :all ->
        run_all_targets()

      {:error, msg} ->
        Mix.shell().error(msg)
        list_targets()
    end
  end

  defp parse_args(args) do
    {opts, positional, invalid} = OptionParser.parse(args, strict: [all: :boolean])

    all? = Keyword.get(opts, :all, false)

    case invalid do
      [] -> parse_selection(all?, positional)
      _ -> {:error, "Unknown options: #{inspect(invalid)}"}
    end
  end

  defp parse_selection(true, []), do: :all

  defp parse_selection(true, _positional) do
    {:error, "Cannot combine --all with a baseline target name"}
  end

  defp parse_selection(false, []), do: :list

  defp parse_selection(false, [name]) do
    if name in @targets do
      {:run, name}
    else
      {:error, "Unknown baseline target: #{name}"}
    end
  end

  defp parse_selection(false, _positional) do
    {:error, "Usage: mix slither.baseline [name|--all]"}
  end

  defp list_targets do
    Mix.shell().info("\nAvailable pure-Python baseline targets:\n")

    for name <- @targets do
      Mix.shell().info("  #{name}")
    end

    Mix.shell().info("\nRun with: mix slither.baseline <name>")
    Mix.shell().info("Run all:  mix slither.baseline --all\n")
  end

  defp run_all_targets do
    failures =
      Enum.reduce(@targets, [], fn name, acc ->
        case run_target(name) do
          :ok -> acc
          {:error, reason} -> [{name, reason} | acc]
        end
      end)
      |> Enum.reverse()

    case failures do
      [] ->
        Mix.shell().info("\nAll pure-Python baselines completed successfully.")

      _ ->
        Mix.shell().error("\nBaseline failures:")

        for {name, reason} <- failures do
          Mix.shell().error("  #{name}: #{inspect(reason)}")
        end

        Mix.raise("One or more baseline targets failed")
    end
  end

  defp run_target(name) do
    Mix.shell().info("\n▸ Running pure-Python baseline: #{name}")

    case Baseline.run(name) do
      {:ok, output} ->
        if output != "", do: Mix.shell().info(output)
        :ok

      {:error, reason, output} ->
        Mix.shell().error("  Baseline error: #{inspect(reason)}")
        if output != "", do: Mix.shell().error(output)
        {:error, reason}
    end
  end
end
