"""Cross-chart interactivity for v3 reports.

Two patterns are supported:

- ``link_brushing(figures, key)`` — embed shared customdata so a hover or
  selection on one chart highlights matching data points in sibling charts.
  Implemented as a small JS snippet that listens to plotly_hover /
  plotly_unhover events and calls Plotly.restyle on every linked div.

- ``cross_filter_panel(facets, target_div_ids)`` — render an HTML filter UI
  (cohort / role / studio tier dropdowns) that toggles ``visible`` on the
  matching traces in every linked chart.

Both helpers are JS-only (no extra dependencies). The output strings can
be appended to a report body after the chart divs.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass


def link_brushing(div_ids: Sequence[str], *, key: str = "person_id") -> str:
    """Return a <script> block that links hover events across the given divs.

    Each plotly trace must already carry ``customdata`` containing the
    shared key. Hovering a point in any linked div emphasises the matching
    points in all other linked divs (opacity 1.0 vs 0.2).
    """
    div_ids_json = json.dumps(list(div_ids))
    return f"""<script>
(function() {{
    var ids = {div_ids_json};
    var KEY = {key!r};
    function highlightIn(divId, kv) {{
        var d = document.getElementById(divId);
        if (!d || !d.data) return;
        var newOpacities = d.data.map(function(trace) {{
            if (!trace.customdata) return null;
            return trace.customdata.map(function(cd) {{
                return (cd === kv ? 1.0 : 0.18);
            }});
        }});
        var update = {{}};
        d.data.forEach(function(_, i) {{
            if (newOpacities[i]) {{
                update["marker.opacity"] = newOpacities[i];
            }}
        }});
        if (Object.keys(update).length) {{
            try {{ Plotly.restyle(divId, update); }} catch (e) {{}}
        }}
    }}
    function clearAll() {{
        ids.forEach(function(id) {{
            var d = document.getElementById(id);
            if (!d || !d.data) return;
            try {{ Plotly.restyle(id, {{"marker.opacity": 1.0}}); }} catch (e) {{}}
        }});
    }}
    function attach() {{
        ids.forEach(function(id) {{
            var d = document.getElementById(id);
            if (!d) return;
            d.on("plotly_hover", function(ev) {{
                if (!ev.points || !ev.points.length) return;
                var cd = ev.points[0].customdata;
                if (cd === undefined || cd === null) return;
                ids.forEach(function(otherId) {{
                    if (otherId !== id) highlightIn(otherId, cd);
                }});
            }});
            d.on("plotly_unhover", clearAll);
        }});
    }}
    if (document.readyState === "loading") {{
        document.addEventListener("DOMContentLoaded", function() {{
            setTimeout(attach, 800);
        }});
    }} else {{
        setTimeout(attach, 800);
    }}
}})();
</script>"""


@dataclass(frozen=True)
class CrossFilterFacet:
    field: str             # e.g. "cohort_decade"
    label: str             # e.g. "デビュー年代"
    options: list[str]     # e.g. ["1990s", "2000s", "2010s", "2020s"]
    default: str = "all"   # "all" = no filter


def cross_filter_panel(
    facets: list[CrossFilterFacet],
    target_div_ids: Sequence[str],
) -> str:
    """Render an HTML/JS filter panel that toggles trace visibility.

    Each trace must carry ``meta = {"facets": {field: value}}`` for the
    panel to know which traces to show / hide. Traces without facet meta
    are always visible (treated as background series).
    """
    # Filter UI
    selectors_html: list[str] = []
    for f in facets:
        opts = "\n".join(
            f'<option value="{o}">{o}</option>' for o in f.options
        )
        selectors_html.append(
            f'<label style="margin-right:1.2rem;font-size:0.85rem;color:#a0a0c0;">'
            f'{f.label}: '
            f'<select data-facet-field="{f.field}" '
            f'style="background:#2a2a4a;color:#e0e0e0;border:1px solid #3a3a5c;'
            f'padding:0.3rem 0.6rem;border-radius:4px;font-size:0.85rem;">'
            f'<option value="all">すべて</option>'
            f"{opts}"
            "</select></label>"
        )

    div_ids_json = json.dumps(list(target_div_ids))
    panel = (
        '<div class="cross-filter-panel" '
        'style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);'
        'padding:0.8rem 1rem;margin-bottom:1.5rem;border-radius:8px;">'
        '<strong style="color:#a0d2db;font-size:0.85rem;margin-right:1rem;">'
        '🔍 フィルタ:</strong>'
        + "".join(selectors_html)
        + "</div>"
    )

    script = f"""<script>
(function() {{
    var ids = {div_ids_json};
    var selects = document.querySelectorAll('.cross-filter-panel select[data-facet-field]');
    function applyFilters() {{
        var filters = {{}};
        selects.forEach(function(s) {{
            var f = s.getAttribute('data-facet-field');
            var v = s.value;
            if (v && v !== 'all') filters[f] = v;
        }});
        ids.forEach(function(id) {{
            var d = document.getElementById(id);
            if (!d || !d.data) return;
            var visible = d.data.map(function(trace) {{
                if (!trace.meta || !trace.meta.facets) return true;
                for (var k in filters) {{
                    if (trace.meta.facets[k] !== filters[k]) return 'legendonly';
                }}
                return true;
            }});
            try {{ Plotly.restyle(id, {{"visible": visible}}); }} catch (e) {{}}
        }});
    }}
    selects.forEach(function(s) {{
        s.addEventListener('change', applyFilters);
    }});
}})();
</script>"""
    return panel + script
