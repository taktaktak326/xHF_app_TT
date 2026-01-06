#!/usr/bin/env python3
"""
Generate a compact version of pref_city.geojson that keeps only the fields the
application needs.

Features from the source GeoJSON are streamed via `ijson`, their properties are
renamed to the keys used in the API (`prefecture`, `municipality`, …), and the
coordinates can optionally be rounded to reduce the byte footprint.

Usage:
    python scripts/compact_pref_city.py \
        --input apps/api/data/pref_city.geojson \
        --output pref_city_compact.geojson \
        --max-decimals 6

Passing an output path that ends in `.gz` produces a gzip-compressed file.
"""

from __future__ import annotations

import argparse
import gzip
import json
from decimal import Decimal
from pathlib import Path
from typing import Any, IO, Iterator

import ijson


KEY_ALIASES = {
    "prefecture": ("prefecture", "N03_001"),
    "prefectureOffice": ("prefectureOffice", "N03_002"),
    "municipality": ("municipality", "N03_003"),
    "subMunicipality": ("subMunicipality", "N03_004"),
    "cityCode": ("cityCode", "N03_007"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to the original pref_city.geojson (supports .gz).",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Path for the compact GeoJSON (append .gz for gzip).",
    )
    parser.add_argument(
        "--max-decimals",
        type=int,
        default=6,
        help="Round coordinates to this many decimal places (default: 6). "
        "Set to a negative value to skip rounding.",
    )
    return parser.parse_args()


def open_text_file(path: Path, mode: str) -> IO[str]:
    if path.suffix == ".gz":
        return gzip.open(path, mode, encoding="utf-8")
    return path.open(mode, encoding="utf-8")


def iter_features(input_path: Path) -> Iterator[dict[str, Any]]:
    with open_text_file(input_path, "rt") as src:
        for feature in ijson.items(src, "features.item"):
            yield feature


def round_coordinates(coords: Any, decimals: int) -> Any:
    if decimals < 0:
        return coords
    if isinstance(coords, (list, tuple)):
        if coords and isinstance(coords[0], (list, tuple)):
            return [round_coordinates(item, decimals) for item in coords]
        if len(coords) >= 2 and all(isinstance(val, (int, float)) for val in coords[:2]):
            return [round(coords[0], decimals), round(coords[1], decimals)] + [
                round_coordinates(item, decimals) for item in coords[2:]
            ]
        return [round_coordinates(item, decimals) for item in coords]
    return coords


def trim_feature(feature: dict[str, Any], decimals: int) -> dict[str, Any] | None:
    geometry = feature.get("geometry")
    if not geometry or geometry.get("type") not in {"Polygon", "MultiPolygon"}:
        return None

    trimmed_geometry = {
        "type": geometry["type"],
        "coordinates": round_coordinates(geometry.get("coordinates"), decimals),
    }

    props = feature.get("properties") or {}
    trimmed_props: dict[str, Any] = {}
    for key, aliases in KEY_ALIASES.items():
        for alias in aliases:
            value = props.get(alias)
            if value not in (None, "", []):
                trimmed_props[key] = value
                break

    return {"type": "Feature", "properties": trimmed_props, "geometry": trimmed_geometry}


def _to_serializable(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def write_feature_collection(output_path: Path, features: Iterator[dict[str, Any]]) -> None:
    with open_text_file(output_path, "wt") as dst:
        dst.write('{"type":"FeatureCollection","features":[')
        first = True
        for feature in features:
            if feature is None:
                continue
            if not first:
                dst.write(",")
            json.dump(feature, dst, ensure_ascii=False, separators=(",", ":"), default=_to_serializable)
            first = False
        dst.write("]}")


def main() -> None:
    args = parse_args()
    decimals = args.max_decimals if args.max_decimals is not None else 6
    features = (trim_feature(f, decimals) for f in iter_features(args.input))
    write_feature_collection(args.output, features)


if __name__ == "__main__":
    main()
