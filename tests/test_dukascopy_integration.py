from datetime import datetime
from pathlib import Path
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from forex_bt.dukascopy_integration import DukascopyModule, _date_to_str


def test_date_to_str_handles_datetime_and_none() -> None:
    assert _date_to_str(datetime(2023, 1, 2)) == "2023-01-02"
    assert _date_to_str(None) == ""


def test_download_adapts_asset_and_dates() -> None:
    class DummyModule:
        def __init__(self) -> None:
            self.calls: list[tuple] = []

        def download(self, assets, start, end="", concurrent=0, force=False):  # type: ignore[no-untyped-def]
            self.calls.append((assets, start, end, concurrent, force))

    module = DummyModule()
    wrapper = DukascopyModule(module)

    wrapper.download("EURUSD", datetime(2023, 1, 1), datetime(2023, 1, 3), concurrent=2, force=True)

    assert module.calls == [(["EURUSD"], "2023-01-01", "2023-01-03", 2, True)]


def test_update_called_without_end_argument() -> None:
    class DummyModule:
        def __init__(self) -> None:
            self.calls: list[tuple] = []

        def update(self, assets, start="", concurrent=0, force=False):  # type: ignore[no-untyped-def]
            self.calls.append((assets, start, concurrent, force))

    module = DummyModule()
    wrapper = DukascopyModule(module)

    wrapper.update("EURUSD", datetime(2023, 2, 1))

    assert module.calls == [(["EURUSD"], "2023-02-01", 0, False)]


def test_set_paths_normalizes_trailing_slash_and_forward_slash() -> None:
    class DummyModule:
        DOWNLOAD_PATH = "./download/"
        EXPORT_PATH = "./export/"

    module = DummyModule()
    wrapper = DukascopyModule(module)

    wrapper.set_paths(download_path=Path("tmp\\downloads"), export_path=Path("tmp/export"))

    assert module.DOWNLOAD_PATH.endswith("tmp/downloads/")
    assert module.EXPORT_PATH.endswith("tmp/export/")
