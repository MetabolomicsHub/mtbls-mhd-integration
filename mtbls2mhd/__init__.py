__version__ = "v0.0.8"

import pathlib
import sys

application_root_path = pathlib.Path(__file__).parent.parent

sys.path.append(str(application_root_path))
