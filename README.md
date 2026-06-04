## S3-snapshot-proxy


### Python package management using uv

Python packages are maintained via pyproject.toml and uv.lock.

To generate a new uv.lock file, run:

`docker compose run --rm uv-lock`

This runs uv inside a container and writes the updated `uv.lock` back into the repo.