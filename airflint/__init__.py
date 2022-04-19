"""Top-level package for airflint."""
from importlib.metadata import version
from os import getcwd
from os.path import dirname, join, realpath

__app_name__ = "airflint"
__version__ = version(__name__)
__location__ = realpath(join(getcwd(), dirname(__file__)))
