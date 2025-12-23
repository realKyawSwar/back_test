from __future__ import annotations

import importlib.util
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

LOG = logging.getLogger(__name__)


def _date_to_str(date: datetime | str | None) -> str:
    """
    Dukascopy script expects YYYY-MM-DD strings. Accept datetime/str/None and
    return the string form ("" means "up to now").
    """

    if date is None:
        return ""
    if isinstance(date, str):
        return date
    return date.strftime("%Y-%m-%d")


def _normalize_path(path: Path) -> str:
    """Convert paths to forward slashes with a trailing slash for the script globals."""

    normalized = path.as_posix().replace("\\", "/")
    if not normalized.endswith("/"):
        normalized += "/"
    return normalized


class DukascopyModule:
    """
    Adapter for the dynamically imported dukascopy-data-manager module.

    The third-party script expects start/end as strings, assets as a list and
    uses module-level DOWNLOAD_PATH/EXPORT_PATH globals. This wrapper keeps the
    rest of the codebase using nicer types (Path, datetime) while translating to
    the script's expectations.
    """

    def __init__(self, module: Any) -> None:
        self.module = module

    @property
    def download_path(self) -> Path:
        return Path(getattr(self.module, "DOWNLOAD_PATH", Path("download")))

    @download_path.setter
    def download_path(self, path: Path) -> None:
        setattr(self.module, "DOWNLOAD_PATH", Path(path))

    @property
    def export_path(self) -> Path:
        return Path(getattr(self.module, "EXPORT_PATH", Path("export")))

    @export_path.setter
    def export_path(self, path: Path) -> None:
        setattr(self.module, "EXPORT_PATH", Path(path))

    def set_paths(self, download_path: Path | None = None, export_path: Path | None = None) -> None:
        """Optionally override dukascopy globals with normalized paths."""

        if download_path is not None and hasattr(self.module, "DOWNLOAD_PATH"):
            setattr(self.module, "DOWNLOAD_PATH", _normalize_path(download_path))
        if export_path is not None and hasattr(self.module, "EXPORT_PATH"):
            setattr(self.module, "EXPORT_PATH", _normalize_path(export_path))

    def download(
        self,
        asset: str,
        start: datetime | str,
        end: datetime | str | None = None,
        *,
        concurrent: int = 0,
        force: bool = False,
    ) -> Any:
        func: Callable[..., Any] | None = getattr(self.module, "download", None)
        if func is None:
            raise AttributeError("dukascopy module has no 'download' function")

        start_str = _date_to_str(start)
        end_str = _date_to_str(end)
        return func([asset], start_str, end=end_str, concurrent=concurrent, force=force)

    def update(
        self,
        asset: str,
        start: datetime | str | None = None,
        *,
        concurrent: int = 0,
        force: bool = False,
    ) -> Any:
        func: Callable[..., Any] | None = getattr(self.module, "update", None)
        if func is None:
            raise AttributeError("dukascopy module has no 'update' function")

        start_str = _date_to_str(start)
        return func([asset], start=start_str, concurrent=concurrent, force=force)


def load_dukas_module(path: Path) -> DukascopyModule:
    """
    Dynamically import the provided dukascopy-data-manager.py script and return a wrapper.
    """

    if not path.is_file():
        raise FileNotFoundError(f"Cannot find dukascopy script at: {path}")

    spec = importlib.util.spec_from_file_location("dukascopy_data_manager", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot import dukascopy script from: {path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    LOG.debug("Loaded dukascopy module from %s", path)
    return DukascopyModule(module)
