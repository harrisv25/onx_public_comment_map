import sys
from pathlib import Path

# Insert the repo root (parent of the tests/ directory) at the front of sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
