import pytest
import os

def pytest_addoption(parser):
    parser.addoption(
        "--scale-factor",
        action="store",
        default=os.environ.get("SCALE_FACTOR", "1"),
        type=float,
        help="Scale factor for the test (default: 1 or value from SCALE_FACTOR environment variable)."
    )

@pytest.fixture
def scale_factor(request):
    return request.config.getoption("--scale-factor")