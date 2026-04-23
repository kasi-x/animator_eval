"""Shared color utility functions for report generators."""


def hex_to_rgba(hex_color: str, alpha: float = 0.3) -> str:
    """Convert hex color to RGBA string.

    Args:
        hex_color: Hex color like "#RRGGBB" or "RRGGBB".
        alpha: Alpha channel value (0.0-1.0).

    Returns:
        RGBA string like "rgba(255,128,0,0.5)".
    """
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"
