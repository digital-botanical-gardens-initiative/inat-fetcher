
import sys
import os

# Add directory containing the inat_fetcher package to the Python path
inat_fetcher_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(inat_fetcher_path)
from inat_fetcher.src.foo import foo

def test_foo():
    assert foo() == "foo"
