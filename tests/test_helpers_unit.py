import pytest

from app.utils.helpers import normalize_tags


@pytest.mark.parametrize(
    "value,expected",
    [
        # GraphQL shape: list of strings
        (["no-sync", "wortmann"], ["no-sync", "wortmann"]),
        ([" no-sync ", "", None, "wortmann"], ["no-sync", "wortmann"]),
        # REST shape: comma separated string
        ("no-sync, wortmann", ["no-sync", "wortmann"]),
        ("no-sync,,  wortmann ,", ["no-sync", "wortmann"]),
        ("no-sync", ["no-sync"]),
        # empty / unexpected input
        (None, []),
        ("", []),
        ([], []),
        ("   ", []),
        (123, []),
    ],
)
def test_normalize_tags(value, expected):
    assert normalize_tags(value) == expected
