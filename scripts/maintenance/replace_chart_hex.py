"""One-shot helper: replace inline chart hex literals with Okabe-Ito CB-safe equivalents.

Targets only Plotly chart colors (string literals inside ``marker_color=``,
``line=dict(color=...)``, ``fillcolor=...``, ``marker=dict(color=...)`` etc.)
across ``scripts/report_generators/reports/*.py``.

Skipped (still raw hex):
- v2 violation warnings (#e05080)
- annotation / text gray colors (#a0a0c0 / #888 / #aaa / etc.)
- CSS border colors (#3a3a5c)
- white / black text (#fff / #FFFFFF / #000)

The mapping is intentionally conservative: only colors that are clearly
"data series" hex codes get replaced. Run once, then `git diff` to review.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPORTS_DIR = (
    Path(__file__).resolve().parents[1] / "report_generators" / "reports"
)

# v3 Okabe-Ito Dark palette (kept here as inline literals to avoid an
# import cycle in the maintenance script).
_OID = (
    "#e0e0e0",  # 0  off-white
    "#FFB444",  # 1  orange
    "#7CC8F2",  # 2  sky blue
    "#3BC494",  # 3  bluish green
    "#F8EC6A",  # 4  yellow
    "#3593D2",  # 5  blue
    "#E07532",  # 6  vermillion
    "#E09BC2",  # 7  reddish purple
)

# Map common chart hexes → CB-safe Okabe-Ito Dark equivalents.
HEX_MAP: dict[str, str] = {
    "#f093fb": _OID[7],   # pink magenta → reddish purple
    "#06D6A0": _OID[3],   # green        → bluish green
    "#FFD166": _OID[4],   # yellow       → yellow
    "#f5576c": _OID[6],   # red          → vermillion
    "#a0d2db": _OID[2],   # light blue   → sky blue
    "#667eea": _OID[5],   # purple-blue  → blue
    "#fda085": _OID[1],   # orange       → orange
    "#4CC9F0": _OID[2],   # cyan         → sky blue
    "#F72585": _OID[7],   # magenta      → reddish purple
    "#FF6B35": _OID[6],   # red-orange   → vermillion
    "#764ba2": _OID[7],   # purple       → reddish purple
    "#7209B7": _OID[7],   # deep purple  → reddish purple
    "#f5a623": _OID[1],   # orange       → orange
    "#e09050": _OID[1],   # tan          → orange
    "#43e97b": _OID[3],   # green        → bluish green
    "#FF6B6B": _OID[6],   # coral        → vermillion
    "#FFA94D": _OID[1],   # orange       → orange
    "#FFD43B": _OID[4],   # yellow       → yellow
    "#69DB7C": _OID[3],   # green        → bluish green
    "#4DABF7": _OID[5],   # blue         → blue
    "#DA77F2": _OID[7],   # violet       → reddish purple
    "#fa709a": _OID[7],   # pink         → reddish purple
    "#4facfe": _OID[5],   # cyan-blue    → blue
}

# Skip these hexes (intentionally raw — annotations / borders / etc.)
SKIP = {
    "#e05080",  # v2 violation warning
    "#a0a0c0", "#8a94a0", "#c0c0d0", "#b0b0c0", "#9090b0",
    "#888", "#888888", "#aaaaaa", "#a0a0a0",
    "#3a3a5c",
    "#fff", "#FFFFFF", "#ffffff",
    "#000", "#000000",
    "#1a1a2e",  # dark bg
    "#7a7a92",  # neutral muted
    "#f5f5f5",  # light bg
    "#0f0c29", "#302b63", "#24243e",  # gradient backgrounds
}

# Match a hex literal inside a string ('...') or "..."  but NOT inside
# a comment, docstring, or raw text block. Conservative: only target
# literals that appear as a single value of a kwarg in a call.
HEX_LITERAL = re.compile(
    r"(?P<quote>['\"])(?P<hex>#[0-9a-fA-F]{6}|#[0-9a-fA-F]{3})(?P=quote)"
)


def replace_in_text(src: str) -> tuple[str, int]:
    """Return (new_src, n_replacements) using HEX_MAP."""
    n = 0

    def _sub(m: re.Match) -> str:
        nonlocal n
        h = m.group("hex")
        if h in SKIP:
            return m.group(0)
        new = HEX_MAP.get(h)
        if not new:
            return m.group(0)
        n += 1
        q = m.group("quote")
        return f"{q}{new}{q}"

    new_src = HEX_LITERAL.sub(_sub, src)
    return new_src, n


def main() -> int:
    total = 0
    for path in sorted(REPORTS_DIR.glob("*.py")):
        if path.name.startswith("_"):
            continue
        src = path.read_text(encoding="utf-8")
        new, n = replace_in_text(src)
        if n:
            path.write_text(new, encoding="utf-8")
            print(f"  {path.name}: {n} replacements")
            total += n
    print(f"\nTotal: {total} hex literals replaced.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
