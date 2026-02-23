defmodule Slither.Examples.Baseline do
  @moduledoc """
  Runs pure-Python comparison baselines for Slither examples.

  Searches for a free-threaded Python interpreter in this order:
    1. `SLITHER_BASELINE_PYTHON` env var
    2. `uv run --python cpython-3.14+freethreaded` (uv manages runtime)
    3. `python3.14t` or `python3.13t` in PATH

  Passes:
    * `--threads <snakepit_pool_size>`
    * `--mode both` (unsafe threaded + safe pure-python)
    * `--require-free-threaded` (hard-fail on GIL runtime)
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
    thread_args = ["--threads", Integer.to_string(baseline_threads())]
    mode_args = ["--mode", "both", "--require-free-threaded"]
    script_args = [script_path, example_name] ++ mode_args ++ thread_args

    case find_strategy() do
      {:direct, python} ->
        run_cmd(python, script_args)

      {:uv} ->
        args = ["run", "--python", "cpython-3.14+freethreaded", "--"] ++ script_args

        run_cmd("uv", args)

      :not_found ->
        {:error, :python_not_found, ""}
    end
  end

  defp baseline_threads do
    case Application.get_env(:snakepit, :pool_config) do
      %{pool_size: size} when is_integer(size) and size > 0 ->
        size

      _ ->
        Application.get_env(:snakepit, :pool_size, @default_threads)
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
        {:direct, env}

      System.find_executable("uv") ->
        {:uv}

      exe = System.find_executable("python3.14t") ->
        {:direct, exe}

      exe = System.find_executable("python3.13t") ->
        {:direct, exe}

      true ->
        :not_found
    end
  end
end
