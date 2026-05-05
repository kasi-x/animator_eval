"""CB-safe palette + fixed category mappings.

Okabe-Ito is a 8-color palette designed to be distinguishable under
deuteranopia / protanopia / tritanopia. Using fixed category mappings
across reports keeps category colors consistent for the reader who
opens multiple reports side-by-side.

Reference:
    Okabe M., Ito K. (2008) "Color Universal Design"
    https://jfly.uni-koeln.de/color/
"""

from __future__ import annotations

from typing import Final

OKABE_ITO: Final[tuple[str, ...]] = (
    "#000000",  # black
    "#E69F00",  # orange
    "#56B4E9",  # sky blue
    "#009E73",  # bluish green
    "#F0E442",  # yellow
    "#0072B2",  # blue
    "#D55E00",  # vermillion
    "#CC79A7",  # reddish purple
)

OKABE_ITO_DARK: Final[tuple[str, ...]] = (
    "#e0e0e0",  # off-white (replaces black on dark bg)
    "#FFB444",
    "#7CC8F2",
    "#3BC494",
    "#F8EC6A",
    "#3593D2",
    "#E07532",
    "#E09BC2",
)


CAREER_STAGE: Final[dict[str, str]] = {
    "初級ランク": "#56B4E9",
    "中級ランク": "#009E73",
    "上級ランク": "#CC79A7",
}

ROLE_GROUP: Final[dict[str, str]] = {
    "animator": "#56B4E9",
    "director": "#CC79A7",
    "designer": "#F0E442",
    "production": "#E69F00",
    "writing": "#009E73",
    "technical": "#0072B2",
    "other": "#7a7a7a",
}

GENDER: Final[dict[str, str]] = {
    "F": "#D55E00",
    "M": "#0072B2",
    "unknown": "#7a7a7a",
}

SIGNIFICANCE: Final[dict[str, str]] = {
    "sig": "#000000",
    "non_sig": "#a0a0a0",
}

NULL_VS_OBSERVED: Final[dict[str, str]] = {
    "observed": "#000000",
    "null": "#a0a0a0",
}


def cohort_decade_color(decade: int, *, dark: bool = False) -> str:
    """Map decade (1970..2020) to viridis color.

    Six bins (1970s..2020s); positions outside fall back to extremes.
    """
    bins = [1970, 1980, 1990, 2000, 2010, 2020]
    palette_dark = ("#440154", "#3b528b", "#21918c", "#5ec962", "#fde725", "#ffd700")
    palette_light = ("#440154", "#3b528b", "#21918c", "#5ec962", "#fde725", "#bcd22e")
    palette = palette_dark if dark else palette_light
    if decade < bins[0]:
        return palette[0]
    if decade >= bins[-1]:
        return palette[-1]
    idx = (decade - bins[0]) // 10
    return palette[int(idx)]


def adjust_for_dark(palette: tuple[str, ...]) -> tuple[str, ...]:
    """Return the dark-bg variant of a palette.

    For Okabe-Ito the only swap needed is black → off-white. Other
    colors retain enough contrast on the glass-morphism dark background.
    """
    return tuple(OKABE_ITO_DARK[i] if c == "#000000" else c for i, c in enumerate(palette))


def hex_to_rgba(hex_color: str, alpha: float = 1.0) -> str:
    """Convert ``#RRGGBB`` to ``rgba(r,g,b,a)`` for use in plotly fillcolor."""
    h = hex_color.lstrip("#")
    if len(h) != 6:
        raise ValueError(f"expected #RRGGBB, got {hex_color!r}")
    r = int(h[0:2], 16)
    g = int(h[2:4], 16)
    b = int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha:.3f})"
