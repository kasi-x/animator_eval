"""Rendering primitives for the declarative report architecture.

Two layers:

- ``html_primitives``: thin wrappers around ``scripts/report_generators/
  html_templates.py`` so that existing CSS / disclaimers / glossary helpers
  are reused verbatim.
- ``chart_renderers``: maps each concrete ``ChartSpec`` dataclass to a
  ``plotly.graph_objects.Figure``.
"""
