import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from threading import Lock


_FILE_HANDLER = None
_FILE_HANDLER_KEY = None
_FILE_HANDLER_LOCK = Lock()


class DailyFileHandler(logging.Handler):
    """Handler fichier avec rotation quotidienne pipeline_YYYYMMDD.log."""

    def __init__(self, logs_dir: Path, file_prefix: str = "pipeline", encoding: str = "utf-8"):
        super().__init__()
        self.logs_dir = logs_dir
        self.file_prefix = file_prefix
        self.encoding = encoding
        self._current_date = None
        self._stream = None

    def _today(self) -> str:
        return datetime.now().strftime("%Y%m%d")

    def _path_for(self, date_str: str) -> Path:
        return self.logs_dir / f"{self.file_prefix}_{date_str}.log"

    def _ensure_stream(self):
        today = self._today()
        if self._stream and self._current_date == today:
            return

        if self._stream:
            self._stream.close()
            self._stream = None

        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self._stream = self._path_for(today).open("a", encoding=self.encoding)
        self._current_date = today

    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            self.acquire()
            try:
                self._ensure_stream()
                self._stream.write(msg + "\n")
                self._stream.flush()
            finally:
                self.release()
        except Exception:
            self.handleError(record)

    def close(self):
        self.acquire()
        try:
            if self._stream:
                self._stream.close()
                self._stream = None
            super().close()
        finally:
            self.release()


def _resolve_logs_dir() -> Path:
    """Retourne le dossier logs (configurable via WIT_LOG_DIR)."""
    default_dir = Path(__file__).resolve().parents[1] / "data" / "logs"
    return Path(os.getenv("WIT_LOG_DIR", str(default_dir)))


def _get_shared_file_handler() -> logging.Handler:
    """Construit/récupère un handler fichier partagé entre loggers."""
    global _FILE_HANDLER, _FILE_HANDLER_KEY

    logs_dir = _resolve_logs_dir()
    file_prefix = os.getenv("WIT_LOG_FILE_PREFIX", "pipeline")
    file_style = os.getenv("WIT_LOG_FILE_STYLE", "compact").lower()
    file_batch_separators = os.getenv("WIT_LOG_FILE_BATCH_SEPARATORS", "0") != "0"
    key = (str(logs_dir), file_prefix, file_style, file_batch_separators)

    with _FILE_HANDLER_LOCK:
        if _FILE_HANDLER is not None and _FILE_HANDLER_KEY == key:
            return _FILE_HANDLER

        handler = DailyFileHandler(logs_dir=logs_dir, file_prefix=file_prefix)
        handler.setFormatter(
            ColorFormatter(
                fmt="%(asctime)s | %(levelname)s | %(name)s",
                datefmt="%Y-%m-%d %H:%M:%S",
                use_color=False,
                style=file_style,
                show_batch_separators=file_batch_separators,
            )
        )
        _FILE_HANDLER = handler
        _FILE_HANDLER_KEY = key
        return _FILE_HANDLER


def get_logger(name: str = "wit"):
    """Retourne un logger configuré (niveau via WIT_LOG_LEVEL)."""
    level = os.getenv("WIT_LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    handler = logging.StreamHandler(stream=sys.stderr)
    use_color = sys.stderr.isatty() and os.getenv("WIT_LOG_COLOR", "1") != "0"
    style = os.getenv("WIT_LOG_STYLE", "compact").lower()
    formatter = ColorFormatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        use_color=use_color,
        style=style,
        show_batch_separators=os.getenv("WIT_LOG_BATCH_SEPARATORS", "1") != "0",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if os.getenv("WIT_LOG_FILE", "1") != "0":
        logger.addHandler(_get_shared_file_handler())

    logger.propagate = False
    return logger


class ColorFormatter(logging.Formatter):
    """Formateur lisible: compact (1 ligne) ou pretty (2 lignes)."""

    LEVEL_COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
    }
    NAME_COLORS = ("\033[34m", "\033[36m", "\033[35m", "\033[32m", "\033[33m")
    STATUS_COLORS = {
        "SKIP": "\033[33m",
        "VALID": "\033[32m",
        "INSERTED": "\033[32m",
        "ERROR": "\033[31m",
        "FAILED": "\033[31m",
        "OK": "\033[32m",
    }
    DIM = "\033[2m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

    def __init__(self, fmt, datefmt=None, use_color=True, style="compact", show_batch_separators=None):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.use_color = use_color
        self.style = style if style in {"compact", "pretty"} else "compact"
        self._status_regex = re.compile(r"\b(" + "|".join(self.STATUS_COLORS.keys()) + r")\b")
        self._batch_start_regex = re.compile(r"^Batch\s+\d+/\d+\b")
        self._batch_summary_regex = re.compile(r"^Batch\s+\d+\s+summary\b")
        self._show_batch_separators = (
            os.getenv("WIT_LOG_BATCH_SEPARATORS", "1") != "0"
            if show_batch_separators is None
            else show_batch_separators
        )
        try:
            self._separator_width = max(40, int(os.getenv("WIT_LOG_SEPARATOR_WIDTH", "104")))
        except ValueError:
            self._separator_width = 104

    def _color_name(self, logger_name: str) -> str:
        """Colorise le nom du logger avec une palette stable."""
        if not self.use_color:
            return logger_name
        idx = sum(ord(c) for c in logger_name) % len(self.NAME_COLORS)
        return f"{self.NAME_COLORS[idx]}{logger_name}{self.RESET}"

    def _indent(self, text: str, prefix: str = "  ") -> str:
        """Indente un message multilignes pour une lecture plus claire."""
        lines = text.splitlines() or [""]
        return "\n".join(f"{prefix}{line}" for line in lines)

    def _format_name(self, name: str, width: int = 28) -> str:
        """Ajuste la largeur du nom de logger pour aligner les colonnes."""
        if len(name) > width:
            return f"{name[:width-1]}…"
        return f"{name:<{width}}"

    def _color_status_keywords(self, message: str) -> str:
        """Colorise certains mots-clés métier dans le message."""
        if not self.use_color:
            return message

        def _replace(match):
            word = match.group(1)
            color = self.STATUS_COLORS.get(word, "")
            return f"{color}{self.BOLD}{word}{self.RESET}" if color else word

        return self._status_regex.sub(_replace, message)

    def _separator_line(self) -> str:
        """Construit une ligne separatrice discrète."""
        line = "─" * self._separator_width
        return f"{self.DIM}{line}{self.RESET}" if self.use_color else line

    def format(self, record: logging.LogRecord) -> str:
        """Construit une sortie lisible pour terminal."""
        timestamp = self.formatTime(record, self.datefmt)
        level = f"{record.levelname:<8}"
        raw_name = record.name
        name = self._format_name(raw_name)
        message = record.getMessage()

        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"
        if record.stack_info:
            message = f"{message}\n{self.formatStack(record.stack_info)}"

        add_prefix_sep = self._show_batch_separators and bool(self._batch_start_regex.match(message))
        add_suffix_sep = self._show_batch_separators and bool(self._batch_summary_regex.match(message))

        header = f"{timestamp} | {level} | {name}"
        if not self.use_color:
            prefix = f"{self._separator_line()}\n" if add_prefix_sep else ""
            suffix = f"\n{self._separator_line()}" if add_suffix_sep else ""
            if self.style == "pretty":
                return f"{prefix}{header}\n{self._indent(message)}{suffix}"
            if "\n" in message:
                lines = message.splitlines()
                rest = "\n".join(lines[1:])
                return (
                    f"{prefix}{header} | {lines[0]}\n"
                    f"{self._indent(rest)}{suffix}"
                )
            return f"{prefix}{header} | {message}{suffix}"

        level_color = self.LEVEL_COLORS.get(record.levelname, "")
        header_colored = (
            f"{self.DIM}{timestamp}{self.RESET} | "
            f"{level_color}{self.BOLD}{level}{self.RESET} | "
            f"{self._color_name(name)}"
        )
        message = self._color_status_keywords(message)
        prefix = f"{self._separator_line()}\n" if add_prefix_sep else ""
        suffix = f"\n{self._separator_line()}" if add_suffix_sep else ""

        if self.style == "pretty":
            message_colored = self._indent(message, prefix=f"{self.DIM}-> {self.RESET}")
            return f"{prefix}{header_colored}\n{message_colored}{suffix}"

        if "\n" in message:
            first, rest = message.splitlines()[0], "\n".join(message.splitlines()[1:])
            return (
                f"{prefix}{header_colored} | {first}\n"
                f"{self._indent(rest, prefix=f'{self.DIM}   {self.RESET}')}{suffix}"
            )
        return f"{prefix}{header_colored} | {message}{suffix}"
