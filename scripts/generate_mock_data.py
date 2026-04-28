import sys
from pathlib import Path
import os

# Determine repo root dynamically or use default
# Priority: environment variable > workspace default
repo_root = os.getenv(
    "REPO_ROOT", 
    "/Workspace/Users/padaialgo@gmail.com/quant-core-poc"
)

REPO_ROOT = Path(repo_root)
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

# Import and run - main() does its own argparse
from quant_core.ingestion.mock_data import main

if __name__ == "__main__":
    main()
