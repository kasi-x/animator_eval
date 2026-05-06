"""1-shot script: 47 都道府県 GeoJSON を取得して data/geo/ に保存する。

出典: dataofjapan/land (MIT License)
  https://github.com/dataofjapan/land

出力: data/geo/japan_prefectures.geojson

使用方法:
    python scripts/maintenance/fetch_jp_geojson.py

次段階作業 (ChoroplethJP 本実装):
    1. src/viz/primitives.py の ChoroplethJP.render() でこのファイルを読み込む。
       ``with open(GEO_PATH) as f: geo = json.load(f)``
    2. 都道府県名 property キーは "nam_ja" (日本語) / "nam" (ローマ字)。
       データフレームとのジョイン時は "nam_ja" を推奨。
    3. Plotly choropleth_mapbox または go.Choropleth で
       geojson=geo, locations=df["nam_ja"], featureidkey="properties.nam_ja"
       を指定する。
    4. テスト: tests/unit/test_viz_choropleth_jp.py を新設し、
       ファイル存在 + Feature count == 47 を assert する。
"""

from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

_GEO_URL = (
    "https://github.com/dataofjapan/land/raw/master/japan.geojson"
)
_OUTPUT_PATH = (
    Path(__file__).parents[2] / "data" / "geo" / "japan_prefectures.geojson"
)


def fetch_geojson(url: str, output_path: Path) -> None:
    """Download the GeoJSON file and save it to output_path.

    Args:
        url: Source URL of the GeoJSON file.
        output_path: Destination file path. Parent directories are created
            automatically.

    Raises:
        urllib.error.URLError: If the network request fails.
        ValueError: If the downloaded content is not valid GeoJSON.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Fetching: {url}", flush=True)
    with urllib.request.urlopen(url, timeout=30) as resp:  # noqa: S310
        raw = resp.read()

    # Validate JSON before writing
    try:
        geo = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Downloaded content is not valid JSON: {exc}") from exc

    feature_count = len(geo.get("features", []))
    if feature_count == 0:
        raise ValueError("GeoJSON contains no features — unexpected content.")

    output_path.write_bytes(raw)
    print(
        f"Saved {feature_count} features → {output_path}",
        flush=True,
    )


def main() -> int:
    try:
        fetch_geojson(_GEO_URL, _OUTPUT_PATH)
    except Exception as exc:  # noqa: BLE001
        print(
            f"[fetch_jp_geojson] Failed: {exc}\n\n"
            "ネットワーク不可の場合は手動で以下コマンドを実行してください:\n"
            f"  mkdir -p {_OUTPUT_PATH.parent}\n"
            f"  wget -O {_OUTPUT_PATH} \\\n"
            f"    '{_GEO_URL}'\n"
            "または:\n"
            f"  curl -L -o {_OUTPUT_PATH} \\\n"
            f"    '{_GEO_URL}'",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
