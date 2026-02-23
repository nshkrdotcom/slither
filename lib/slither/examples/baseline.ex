defmodule Slither.Examples.Baseline do
  @moduledoc """
  Runs pure-Python threaded baseline equivalents for Slither examples.

  Searches for a free-threaded Python interpreter in this order:
    1. `SLITHER_BASELINE_PYTHON` env var
    2. `python3.14t` or `python3.13t` in PATH
    3. `uv run --python cpython-3.14+freethreaded` (uv manages the runtime)
    4. System `python3` / `python` with a warning

  Passes `--threads 48` to match Slither's worker count.
  """

  @script_rel_path "priv/python/examples/pure_python_baselines.py"
  @default_threads 48

  @type result ::
          {:ok, String.t()}
          | {:error, term(), String.t()}

  @spec run(String.t()) :: result()
  def run(example_name) when is_binary(example_name) do
    script_path = Path.join(File.cwd!(), @script_rel_path)

    if File.exists?(script_path) do
      invoke(script_path, example_name)
    else
      {:error, :missing_script, ""}
    end
  end

  defp invoke(script_path, example_name) do
    thread_args = ["--threads", Integer.to_string(@default_threads)]

    case find_strategy() do
      {:direct, python, warning} ->
        if warning, do: IO.puts("  #{warning}")
        args = [script_path, example_name | thread_args]
        run_cmd(python, args)

      {:uv, warning} ->
        if warning, do: IO.puts("  #{warning}")

        args =
          ["run", "--python", "cpython-3.14+freethreaded", "--", script_path, example_name] ++
            thread_args

        run_cmd("uv", args)

      :not_found ->
        {:error, :python_not_found, ""}
    end
  end

  defp run_cmd(cmd, args) do
    {output, status} = System.cmd(cmd, args, stderr_to_stdout: true)
    trimmed = String.trim_trailing(output)

    case status do
      0 -> {:ok, trimmed}
      _ -> {:error, {:exit_status, status}, trimmed}
    end
  end

  defp find_strategy do
    cond do
      env = System.get_env("SLITHER_BASELINE_PYTHON") ->
        {:direct, env, nil}

      exe = System.find_executable("python3.14t") ->
        {:direct, exe, nil}

      exe = System.find_executable("python3.13t") ->
        {:direct, exe, nil}

      System.find_executable("uv") ->
        {:uv, nil}

      exe = System.find_executable("python3") || System.find_executable("python") ->
        {:direct, exe,
         "WARNING: Using #{exe} (not free-threaded). Install uv or set SLITHER_BASELINE_PYTHON."}

      true ->
        :not_found
    end
  end
end
