import Config

# Enable Snakepit Python worker pool for SnakeBridge calls.
# This finds the venv, sets PYTHONPATH, configures the adapter, and starts workers.
SnakeBridge.ConfigHelper.configure_snakepit!(pool_size: 2)
