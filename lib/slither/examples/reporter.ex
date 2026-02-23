defmodule Slither.Examples.Reporter do
  @moduledoc """
  Shared formatting helpers for Slither examples.
  """

  @doc """
  Print elapsed time from a `:timer.tc/1` result (microseconds).
  """
  def print_timing(label, microseconds) do
    ms = div(microseconds, 1000)

    formatted =
      cond do
        ms < 1000 -> "#{ms}ms"
        ms < 60_000 -> "#{Float.round(ms / 1000, 1)}s"
        true -> "#{div(ms, 60_000)}m #{rem(div(ms, 1000), 60)}s"
      end

    IO.puts("  #{label}: #{formatted}")
  end

  @doc """
  Print throughput as items/sec.
  """
  def print_throughput(label, {count, microseconds}) do
    seconds = microseconds / 1_000_000
    rate = Float.round(count / max(seconds, 0.001), 1)
    IO.puts("  #{label}: #{rate} items/sec (#{count} items in #{Float.round(seconds, 2)}s)")
  end

  @doc """
  Print a correctness check: expected vs observed with loss percentage.
  """
  def print_correctness(label, expected, observed) do
    diff = expected - observed
    pct = if expected > 0, do: Float.round(abs(diff) / expected * 100, 1), else: 0.0

    status = if diff == 0, do: "OK", else: "MISMATCH"

    IO.puts(
      "  #{label}: expected=#{expected} observed=#{observed} " <>
        "diff=#{diff} (#{pct}%) [#{status}]"
    )
  end

  @doc """
  Print system info: CPU cores, worker count, Python version.
  """
  def print_system_info do
    cores = System.schedulers_online()

    pool_size =
      case Application.get_env(:snakepit, :pool_config) do
        %{pool_size: size} -> size
        _ -> Application.get_env(:snakepit, :pool_size, "?")
      end

    python_version =
      case System.cmd("python3", ["--version"], stderr_to_stdout: true) do
        {version, 0} -> String.trim(version)
        _ -> "unknown"
      end

    IO.puts("=== System Info ===")
    IO.puts("  CPU cores (schedulers): #{cores}")
    IO.puts("  Snakepit workers:       #{pool_size}")
    IO.puts("  Python (workers):       #{python_version}")

    baseline_python = find_baseline_python()

    if baseline_python do
      case System.cmd(baseline_python, ["--version"], stderr_to_stdout: true) do
        {version, 0} ->
          IO.puts("  Python (baseline):      #{String.trim(version)} (#{baseline_python})")

        _ ->
          IO.puts("  Python (baseline):      #{baseline_python}")
      end
    else
      IO.puts("  Python (baseline):      (will use uv run)")
    end

    IO.puts("")
  end

  defp find_baseline_python do
    cond do
      env = System.get_env("SLITHER_BASELINE_PYTHON") ->
        env

      exe = System.find_executable("python3.14t") ->
        exe

      exe = System.find_executable("python3.13t") ->
        exe

      System.find_executable("uv") ->
        case System.cmd("uv", ["python", "find", "cpython-3.14+freethreaded"],
               stderr_to_stdout: true
             ) do
          {path, 0} -> String.trim(path)
          _ -> nil
        end

      true ->
        nil
    end
  end
end
