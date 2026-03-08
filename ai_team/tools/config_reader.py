"""Tool for reading WIT V1 config sections."""
import ast
import importlib.util
from pathlib import Path
from crewai.tools import BaseTool

ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = ROOT / "smart_wallet_analysis" / "config.py"


def _load_config_source() -> str:
    return CONFIG_PATH.read_text(encoding="utf-8")


class ConfigReaderTool(BaseTool):
    """Read a specific section of config.py."""

    name: str = "config_reader"
    description: str = (
        "Read a configuration section from config.py. "
        "Input: section name (e.g. 'SCORE_ENGINE', 'TRACKING_LIVE', 'PIPELINES'). "
        "Returns the raw config dict as text."
    )

    def _run(self, section: str) -> str:
        section = section.strip().upper()
        source = _load_config_source()
        # Extract the variable definition block
        lines = source.splitlines()
        result = []
        in_block = False
        depth = 0
        for line in lines:
            if line.startswith(f"{section} = {{") or line.startswith(f"{section}={{"):
                in_block = True
                depth = 0
            if in_block:
                result.append(line)
                depth += line.count("{") - line.count("}")
                if depth <= 0 and result:
                    break
        if not result:
            # List available sections
            sections = [
                l.split("=")[0].strip()
                for l in lines
                if "= {" in l and not l.startswith(" ") and not l.startswith("#")
            ]
            return f"Section '{section}' not found. Available: {', '.join(sections)}"
        return "\n".join(result)


class ConfigFullReaderTool(BaseTool):
    """Read the full config.py file."""

    name: str = "config_full_reader"
    description: str = (
        "Read the entire config.py file to understand all available parameters. "
        "No input required (pass empty string). "
        "Returns full config.py contents."
    )

    def _run(self, _: str = "") -> str:
        return _load_config_source()
