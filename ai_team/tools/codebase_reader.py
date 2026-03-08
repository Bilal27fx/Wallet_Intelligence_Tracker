"""Tool for reading WIT V1 source files."""
import os
from pathlib import Path
from crewai.tools import BaseTool
from pydantic import Field

ROOT = Path(__file__).resolve().parent.parent.parent
SRC = ROOT / "smart_wallet_analysis"

ALLOWED_EXTENSIONS = {".py", ".md", ".yml", ".yaml"}


class CodebaseReaderTool(BaseTool):
    """Read source files from the WIT V1 codebase."""

    name: str = "codebase_reader"
    description: str = (
        "Read a source file from the WIT V1 project. "
        "Input: relative path from project root (e.g. 'smart_wallet_analysis/config.py'). "
        "Returns file contents as text."
    )

    def _run(self, relative_path: str) -> str:
        path = ROOT / relative_path.strip()
        if not path.exists():
            return f"ERROR: File not found: {path}"
        if path.suffix not in ALLOWED_EXTENSIONS:
            return f"ERROR: Extension not allowed: {path.suffix}"
        try:
            return path.read_text(encoding="utf-8")
        except Exception as e:
            return f"ERROR reading {path}: {e}"


class CodebaseListTool(BaseTool):
    """List Python files in a WIT V1 module directory."""

    name: str = "codebase_list"
    description: str = (
        "List all Python files in a WIT V1 module. "
        "Input: module name (e.g. 'score_engine', 'tracking_live', 'consensus_live'). "
        "Returns list of file paths."
    )

    def _run(self, module_name: str) -> str:
        module_path = SRC / module_name.strip()
        if not module_path.exists():
            # Try root level
            module_path = ROOT / module_name.strip()
        if not module_path.is_dir():
            return f"ERROR: Module directory not found: {module_name}"
        files = sorted(module_path.rglob("*.py"))
        if not files:
            return f"No Python files found in {module_name}"
        return "\n".join(str(f.relative_to(ROOT)) for f in files)
