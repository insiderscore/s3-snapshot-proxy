## S3-snapshot-proxy


### Python package management using uv

Python packages are maintained via pyproject.toml and uv.lock.

To generate a new uv.lock file, run:

`docker compose run --rm uv-lock`

This runs uv inside a container and writes the updated `uv.lock` back into the repo.

### Async S3 client follow-up

The proxy streams object bodies through `httpx.AsyncClient`, but still uses boto3
for some control-plane S3 operations such as version listing, object metadata
checks, tagging reads, and multi-object delete. These blocking boto3 calls are
currently offloaded with `asyncio.to_thread` so they do not block the FastAPI
event loop.

TODO: replace the thread offload with purpose-built async S3 requests over
`httpx.AsyncClient`, or evaluate an async boto-compatible client once it is a
better fit for the proxy's streaming and signing requirements.
