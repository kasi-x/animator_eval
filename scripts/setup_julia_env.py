"""Julia/CairoMakie 環境セットアップスクリプト.

juliacall 経由で Julia を起動し、julia_viz/Project.toml の
依存パッケージをインストールする。

Usage:
    pixi run -e viz setup-julia
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
JULIA_VIZ_DIR = PROJECT_ROOT / "julia_viz"


def main() -> None:
    print(f"[setup-julia] Activating Julia project: {JULIA_VIZ_DIR}")

    try:
        from juliacall import Main as jl  # type: ignore[import-untyped]
    except ImportError:
        print("ERROR: juliacall is not installed.")
        print("Install via: pixi install -e viz")
        sys.exit(1)

    # プロジェクトをアクティベートし依存をインストール
    # Note: juliacall の seval() は Julia コードを実行する公式 API であり、
    # Python の eval() とは異なる。パッケージ管理にのみ使用。
    activate_cmd = f'using Pkg; Pkg.activate("{JULIA_VIZ_DIR}"); Pkg.instantiate()'
    print("[setup-julia] Running Pkg.activate + Pkg.instantiate()...")
    jl.seval(activate_cmd)  # noqa: S307

    # CairoMakie のプリコンパイルを実行
    print("[setup-julia] Precompiling CairoMakie (this may take 2-5 minutes)...")
    jl.seval("using CairoMakie")  # noqa: S307

    print("[setup-julia] Done! Julia environment is ready.")


if __name__ == "__main__":
    main()
