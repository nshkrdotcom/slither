defmodule Slither.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/nshkrdotcom/slither"

  def project do
    [
      app: :slither,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),

      # Hex
      description: description(),
      package: package(),
      source_url: @source_url,

      # Docs
      name: "Slither",
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Slither.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:snakebridge, "~> 0.16.0"},
      {:snakepit, "~> 0.13.0"},
      {:gen_stage, "~> 1.2"},
      {:telemetry, "~> 1.2"},
      {:nimble_options, "~> 1.1"},

      # Dev/Test
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false}
    ]
  end

  defp description do
    """
    Low-level BEAM↔Python concurrency substrate. ETS-backed shared state with
    Python views, batched fan-out with real backpressure, and stage composition
    over BEAM + Python steps. Built on SnakeBridge/Snakepit.
    """
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url
      }
    ]
  end

  defp docs do
    [
      main: "Slither",
      source_ref: "v#{@version}",
      source_url: @source_url
    ]
  end
end
