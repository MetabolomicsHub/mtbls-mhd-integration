__version__ = "v1.0.0"

import pathlib
import sys

application_root_path = pathlib.Path(__file__).parent.parent

sys.path.append(str(application_root_path))
