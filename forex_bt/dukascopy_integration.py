from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from typing import Any, Callable

LOG = logging.getLogger(__name__)


class DukascopyModule:
    """Wrapper around the dynamically imported dukascopy-data-manager module."""

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

    def download(self, *args: Any, **kwargs: Any) -> Any:
        func: Callable[..., Any] | None = getattr(self.module, "download", None)
        if func is None:
            raise AttributeError("dukascopy module has no 'download' function")
        return func(*args, **kwargs)

    def update(self, *args: Any, **kwargs: Any) -> Any:
        func: Callable[..., Any] | None = getattr(self.module, "update", None)
        if func is None:
            raise AttributeError("dukascopy module has no 'update' function")
        return func(*args, **kwargs)


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
