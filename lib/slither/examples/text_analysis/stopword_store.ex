defmodule Slither.Examples.TextAnalysis.StopwordStore do
  @moduledoc """
  ETS-backed stopword list for the text analysis pipeline example.

  Loads a comprehensive set of English stopwords into a `:set` table
  with read concurrency enabled. The store exposes a `get_stopwords`
  view so Python stages can retrieve the full list for filtering.
  """

  @behaviour Slither.Store

  alias Slither.Store.Server

  @stopwords ~w(
    a about above after again against all am an and any are aren't as at
    be because been before being below between both but by
    can cannot could couldn't
    dare did didn't do does doesn't doing don't down during
    each even every
    few for from further
    get got
    had hadn't has hasn't have haven't having he he'd he'll he's her here
    here's hers herself him himself his how how's
    i i'd i'll i'm i've if in into is isn't it it's its itself
    just
    let's
    may me might mine more most mustn't my myself
    need neither no nor not
    of off on once only or other ought our ours ourselves out over own
    same shall shan't she she'd she'll she's should shouldn't so some such
    than that that's the their theirs them themselves then there there's
    these they they'd they'll they're they've this those through to too
    under until up upon us
    very
    was wasn't we we'd we'll we're we've were weren't what what's when
    when's where where's which while who who's whom why why's will with
    won't would wouldn't
    yes yet you you'd you'll you're you've your yours yourself yourselves
  )

  @impl true
  def tables do
    [
      %{
        name: :stopwords,
        type: :set,
        keypos: 1,
        read_concurrency: true,
        write_concurrency: false,
        compressed: false
      }
    ]
  end

  @impl true
  def views do
    [
      %{
        name: :get_stopwords,
        mode: :scalar,
        scope: :session,
        timeout_ms: 5_000,
        handler: fn _params, _ctx ->
          words =
            Server.fold(__MODULE__, :stopwords, [], fn {word, _}, acc ->
              [word | acc]
            end)

          %{"stopwords" => words}
        end
      }
    ]
  end

  @impl true
  def load(tables) do
    for word <- @stopwords do
      :ets.insert(tables[:stopwords], {word, true})
    end

    :ok
  end
end
