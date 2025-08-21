from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()

CLAIMS_DIR = Path(os.getenv("CLAIMS_DIR")) # type: ignore