## S3-snapshot-proxy


### Python package management using uv

Python packages are maintained via pyproject.toml and uv.lock.
To generate a new uv.lock file, run `uv lock --python 3.11`
Substitute the appropriate python version based on the Docker image we are using.

To install uv:

`curl -LsSf https://astral.sh/uv/install.sh | sh`