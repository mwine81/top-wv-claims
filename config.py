from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()

CLAIMS_DIR = Path(os.getenv("STATE_REPORTS")) # type: ignore
NADAC_FILES = Path(os.getenv("NADAC_DIR")) / 'NADAC*.parquet' #type: ignore
MEDISPAN_FILE = Path(os.getenv("MEDISPAN_FILE")) # type: ignore