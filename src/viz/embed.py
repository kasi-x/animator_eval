"""HTML embed for plotly figures (back-compat shim for plotly_div_safe).

The legacy ``scripts.report_generators.html_templates.plotly_div_safe``
delegates to this module. Reports MUST eventually call ``viz.embed``
directly; the legacy name is kept only for migration.
"""

from __future__ import annotations

import base64

import plotly.graph_objects as go


def embed(fig: go.Figure, div_id: str, *, height: int = 480) -> str:
    """Render ``fig`` as a lazy-loaded HTML div.

    Uses base64 + IntersectionObserver to avoid rendering off-screen
    charts. Compatible drop-in for ``plotly_div_safe``.
    """
    if "height" not in fig.layout or fig.layout.height is None:
        fig.update_layout(height=height)
    encoded = base64.b64encode(fig.to_json().encode()).decode()
    return f"""<div class="chart-container">
<div id="{div_id}" data-b64="{encoded}" style="min-height:{height}px;"></div>
<script>
(function() {{
    var el = document.getElementById("{div_id}");
    var done = false;
    function doRender() {{
        if (done) return;
        var b64 = el.getAttribute("data-b64");
        if (!b64) {{ done = true; return; }}
        el.removeAttribute("data-b64");
        done = true;
        var d = JSON.parse(atob(b64));
        return Plotly.newPlot("{div_id}", d.data, d.layout,
                       {{responsive: true, displayModeBar: true}});
    }}
    if (typeof IntersectionObserver !== "undefined") {{
        var obs = new IntersectionObserver(function(entries) {{
            if (done || !entries[0].isIntersecting) return;
            obs.disconnect();
            if (typeof queuePlot === "function") {{
                queuePlot(doRender);
            }} else {{
                doRender();
            }}
        }}, {{rootMargin: "200px"}});
        obs.observe(el);
    }} else {{
        doRender();
    }}
}})();
</script>
</div>"""
