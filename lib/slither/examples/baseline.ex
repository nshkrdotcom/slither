defmodule Slither.Examples.Baseline do
  @moduledoc """
  Runs pure-Python threaded baseline equivalents for Slither examples.

  The baseline harness intentionally uses shared mutable state with no
  synchronization to demonstrate failure modes that Slither avoids via
  process isolation.
  """

  @script_rel_path "priv/python/examples/pure_python_baselines.py"

  @type result ::
          {:ok, String.t()}
          | {:error, term(), String.t()}

  @spec run(String.t()) :: result()
  def run(example_name) when is_binary(example_name) do
    script_path = Path.join(File.cwd!(), @script_rel_path)

    if File.exists?(script_path) do
      invoke_python(script_path, example_name)
    else
      {:error, :missing_script, ""}
    end
  end

  defp invoke_python(script_path, example_name) do
    case python_executable() do
      nil ->
        {:error, :python_not_found, ""}

      python ->
        {output, status} =
          System.cmd(python, [script_path, example_name], stderr_to_stdout: true)

        trimmed = String.trim_trailing(output)

        case status do
          0 -> {:ok, trimmed}
          _ -> {:error, {:exit_status, status}, trimmed}
        end
    end
  end

  defp python_executable do
    Application.get_env(:snakepit, :python_executable) ||
      System.find_executable("python3") ||
      System.find_executable("python")
  end
end
