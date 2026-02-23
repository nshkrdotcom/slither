defmodule Slither.Examples.MlScoring.FeatureStore do
  @moduledoc """
  Write-through feature cache for the ML scoring pipeline example.

  Provides a single `:set` ETS table with both read and write concurrency
  enabled. The table starts empty and is populated during pipeline execution
  as records are featurized. Subsequent runs can hit the cache to skip the
  Python featurization stage for already-seen records.
  """

  @behaviour Slither.Store

  @impl true
  def tables do
    [
      %{
        name: :feature_cache,
        type: :set,
        keypos: 1,
        read_concurrency: true,
        write_concurrency: true,
        compressed: false
      }
    ]
  end

  @impl true
  def views, do: []

  @impl true
  def load(_tables), do: :ok
end
