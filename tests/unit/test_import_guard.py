"""Runtime import guard tests for analysis/pipeline layer boundaries."""

from __future__ import annotations

import pytest

import src.analysis  # noqa: F401  # installs guard at import time


def test_analysis_layer_cannot_import_display_lookup():
    with pytest.raises(ImportError, match="must not import"):
        exec(  # noqa: S102
            "from src.utils.display_lookup import get_display_score",
            {"__name__": "src.analysis._guard_probe"},
        )


def test_pipeline_layer_cannot_import_display_lookup():
    with pytest.raises(ImportError, match="must not import"):
        exec(  # noqa: S102
            "from src.utils.display_lookup import get_display_score",
            {"__name__": "src.pipeline_phases._guard_probe"},
        )


def test_non_analysis_import_of_display_lookup_is_allowed():
    exec(  # noqa: S102
        "from src.utils.display_lookup import get_display_score",
        {"__name__": "scripts._guard_probe"},
    )
