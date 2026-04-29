"""Pipeline entry point compatibility module.

Redirects to src.runtime.pipeline for backward compatibility with pixi tasks.
"""

from src.runtime.pipeline import main

if __name__ == "__main__":
    main()