import Config

config :slither,
  stores: [],
  dispatch: [
    default_batch_size: 64,
    default_max_in_flight: 8,
    default_ordering: :preserve,
    default_on_error: :halt
  ],
  bridge: [
    default_scope: :session
  ]

import_config "#{config_env()}.exs"
