"""Import boundary guards for architecture-layer constraints."""

from __future__ import annotations

import builtins
import inspect


_FORBIDDEN_PREFIX = "src.utils.display_lookup"
_RESTRICTED_CALLER_PREFIXES = ("src.analysis", "src.pipeline_phases")


_ORIGINAL_IMPORT = builtins.__import__
_GUARD_INSTALLED = False


def _guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name == _FORBIDDEN_PREFIX or name.startswith(f"{_FORBIDDEN_PREFIX}."):
        module_name = ""
        if globals:
            module_name = globals.get("__name__", "")
        if not module_name:
            for frame_info in inspect.stack(context=0):
                module_name = frame_info.frame.f_globals.get("__name__", "")
                if module_name:
                    break
        if module_name.startswith(_RESTRICTED_CALLER_PREFIXES):
            raise ImportError(
                f"{module_name} must not import {name}. "
                "display_lookup reads bronze tables; analysis/pipeline must stay on silver."
            )
    return _ORIGINAL_IMPORT(name, globals, locals, fromlist, level)


def install_display_lookup_boundary_guard() -> None:
    """Install runtime import guard once per interpreter process."""
    global _GUARD_INSTALLED
    if _GUARD_INSTALLED:
        return
    builtins.__import__ = _guarded_import
    _GUARD_INSTALLED = True
