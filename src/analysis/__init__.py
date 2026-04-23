"""Analysis package boundary guard + backward compatibility re-export."""

from src.utils.import_guard import install_display_lookup_boundary_guard

install_display_lookup_boundary_guard()

# Backward compatibility: re-export from subpackages
try:
    from src.analysis.entity import *  # noqa: F401, F403
except ImportError:
    pass
