import asyncio
import gzip
import json
import logging
import os
import time
import threading
from array import array
from pathlib import Path
from typing import Any, Dict, IO, Iterable, List, Optional, Tuple

import ijson

logger = logging.getLogger(__name__)

MAX_POLYGON_POINTS = int(os.getenv("PREF_CITY_MAX_POINTS", "200"))
PREF_CITY_ENABLED = os.getenv("PREF_CITY_ENABLED", "false").lower() in ("1", "true", "yes", "on")

_pref_city_index: Optional[List[Dict[str, Any]]] = None
_pref_city_lock = threading.Lock()
_pref_city_source_path: Optional[Path] = None
_pref_city_loading: bool = False
_pref_city_load_event = threading.Event()
_pref_city_load_event.set()
_pref_city_last_error: Optional[str] = None


def _normalize_lat_lon(lat: float, lon: float) -> Tuple[float, float]:
    if abs(lat) > 90.0 and abs(lon) <= 90.0:
        return lon, lat
    return lat, lon


def _resolve_pref_city_source() -> Optional[Path]:
    global _pref_city_source_path  # pylint: disable=global-statement

    if _pref_city_source_path and _pref_city_source_path.exists():
        return _pref_city_source_path

    if not PREF_CITY_ENABLED:
        return None

    path: Optional[Path] = None
    base_dir = Path(__file__).resolve().parent.parent
    repo_root = base_dir.parent.parent
    local_candidates = [
        repo_root / "pref_city_compact.geojson.gz",
        base_dir / "data" / "pref_city_compact.geojson.gz",
        base_dir / "data" / "pref_city.geojson",
    ]
    for local_candidate in local_candidates:
        if local_candidate.exists():
            path = local_candidate
            break

    if path:
        _pref_city_source_path = path
    else:
        logger.error(
            "pref_city dataset not found. Keep pref_city_compact.geojson.gz "
            "at repo root or data/pref_city.geojson for local development."
        )
    return path


def _ensure_closed_ring(points: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    if not points:
        return points
    if points[0] != points[-1]:
        return points + [points[0]]
    return points


def _normalize_point(pt: Any) -> Optional[Tuple[float, float]]:
    if not isinstance(pt, (list, tuple)) or len(pt) < 2:
        return None
    try:
        lon = float(pt[0])
        lat = float(pt[1])
    except (TypeError, ValueError):
        return None
    return _normalize_lat_lon(lat, lon)[::-1]


def _polygon_area_and_centroid(points: List[Tuple[float, float]]) -> Tuple[float, float, float]:
    points = _ensure_closed_ring(points)
    if len(points) < 3:
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        if not xs or not ys:
            return 0.0, 0.0, 0.0
        return 0.0, sum(xs) / len(xs), sum(ys) / len(ys)

    area = 0.0
    cx = 0.0
    cy = 0.0
    for i in range(len(points) - 1):
        x0, y0 = points[i]
        x1, y1 = points[i + 1]
        cross = (x0 * y1) - (x1 * y0)
        area += cross
        cx += (x0 + x1) * cross
        cy += (y0 + y1) * cross

    area *= 0.5
    if abs(area) < 1e-12:
        xs = [p[0] for p in points[:-1]]
        ys = [p[1] for p in points[:-1]]
        if not xs or not ys:
            return 0.0, points[0][0], points[0][1]
        return 0.0, sum(xs) / len(xs), sum(ys) / len(ys)

    cx /= (6.0 * area)
    cy /= (6.0 * area)
    return area, cx, cy


def _reduce_ring(points: List[Tuple[float, float]], max_points: int) -> List[Tuple[float, float]]:
    points = _ensure_closed_ring(points)
    if len(points) <= max_points:
        return points
    step = max(1, len(points) // max_points)
    reduced = [points[i] for i in range(0, len(points), step)]
    if reduced[-1] != points[-1]:
        reduced.append(points[-1])
    return _ensure_closed_ring(reduced)


def _to_compact_arrays(points: List[Tuple[float, float]]) -> Tuple[array, array]:
    lon_arr = array("f")
    lat_arr = array("f")
    for lon, lat in points:
        lon_arr.append(float(lon))
        lat_arr.append(float(lat))
    return lon_arr, lat_arr


def _iter_exterior_rings(geometry: Dict[str, Any]) -> List[List[Any]]:
    geom_type = geometry.get("type")
    coordinates = geometry.get("coordinates")
    if not coordinates:
        return []
    rings: List[List[Any]] = []
    if geom_type == "Polygon":
        if coordinates:
            rings.append(coordinates[0])
    elif geom_type == "MultiPolygon":
        for polygon in coordinates:
            if polygon:
                rings.append(polygon[0])
    return rings


def _process_feature(feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    geometry_dict = feature.get("geometry")
    if not geometry_dict:
        return None

    polygons: List[Tuple[array, array]] = []
    minx = float("inf")
    miny = float("inf")
    maxx = float("-inf")
    maxy = float("-inf")
    total_area = 0.0
    weighted_cx = 0.0
    weighted_cy = 0.0

    for raw_ring in _iter_exterior_rings(geometry_dict):
        normalized: List[Tuple[float, float]] = []
        for pt in raw_ring or []:
            mapped = _normalize_point(pt)
            if mapped:
                normalized.append(mapped)
        if len(normalized) < 3:
            continue

        normalized = _ensure_closed_ring(normalized)
        for lon, lat in normalized:
            minx = min(minx, lon)
            miny = min(miny, lat)
            maxx = max(maxx, lon)
            maxy = max(maxy, lat)

        area, cx, cy = _polygon_area_and_centroid(normalized)
        weight = abs(area) or 1e-9
        total_area += weight
        weighted_cx += cx * weight
        weighted_cy += cy * weight

        reduced = _reduce_ring(normalized, MAX_POLYGON_POINTS)
        polygons.append(_to_compact_arrays(reduced))

    if not polygons:
        return None

    centroid_lon = weighted_cx / total_area
    centroid_lat = weighted_cy / total_area
    centroid_lat, centroid_lon = _normalize_lat_lon(centroid_lat, centroid_lon)

    props = feature.get("properties") or {}
    prefecture = props.get("prefecture") or props.get("N03_001") or None
    prefecture_office = props.get("prefectureOffice") or props.get("N03_002") or None
    raw_municipality = props.get("municipality") or props.get("N03_003")
    raw_sub = props.get("subMunicipality") or props.get("N03_004")
    city_code = props.get("cityCode") or props.get("N03_007") or None

    if raw_municipality:
        municipality = raw_municipality
        sub_municipality = raw_sub or None
    else:
        municipality = raw_sub or None
        sub_municipality = None

    if not prefecture and not municipality and not sub_municipality:
        return None

    return {
        "prefecture": prefecture,
        "prefectureOffice": prefecture_office,
        "municipality": municipality,
        "subMunicipality": sub_municipality,
        "cityCode": city_code,
        "polygons": polygons,
        "bbox": (minx, miny, maxx, maxy),
        "centroid": (centroid_lon, centroid_lat),
    }


def _iter_pref_city_entries(fp: IO[str], format_hint: str) -> Iterable[Dict[str, Any]]:
    """Iterate entries from geojson or jsonl."""
    if format_hint == "jsonl":
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                yield record
            except json.JSONDecodeError:
                continue
    else:
        for feature in ijson.items(fp, "features.item"):
            yield feature


def _detect_format_from_name(name: Optional[str]) -> str:
    if not name:
        return "geojson"
    lower = name.lower()
    if lower.endswith(".jsonl") or lower.endswith(".jsonl.gz"):
        return "jsonl"
    return "geojson"


def _process_entry_to_index(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # If entry already looks like processed (jsonl output), return as-is.
    if (
        "bbox" in entry
        and "polygons" in entry
        and isinstance(entry.get("polygons"), list)
        and isinstance(entry.get("bbox"), (list, tuple))
    ):
        return entry
    # Otherwise process raw GeoJSON feature
    return _process_feature(entry)


def _load_pref_city_index() -> List[Dict[str, Any]]:
    if not PREF_CITY_ENABLED:
        logger.info("pref_city: disabled (PREF_CITY_ENABLED=false)")
        return []

    started_total = time.perf_counter()
    index: List[Dict[str, Any]] = []

    # Load from local file only (no GCS dependency)
    source_path = _resolve_pref_city_source()
    if not source_path:
        logger.info("pref_city: source not configured")
        return []

    fmt = _detect_format_from_name(str(source_path))
    logger.info("pref_city: local load start (path=%s, format=%s)", source_path, fmt)
    try:
        if source_path.suffix == ".gz":
            fp_raw = gzip.open(source_path, "rt", encoding="utf-8")
        else:
            fp_raw = source_path.open("r", encoding="utf-8")
        with fp_raw as fp:
            for feature in _iter_pref_city_entries(fp, fmt):
                entry = _process_entry_to_index(feature)
                if entry:
                    index.append(entry)
    except FileNotFoundError:
        logger.error("pref_city dataset not found at %s", source_path)
    except json.JSONDecodeError as exc:
        logger.error("pref_city dataset is invalid (%s)", exc)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Failed to load pref_city dataset: %s", exc)

    if len(index) == 0:
        logger.warning("pref_city dataset is empty (path=%s)", source_path)
        return []

    total_elapsed = time.perf_counter() - started_total
    logger.info(
        "Loaded %d pref/city entries from %s (total %.2fs)",
        len(index),
        source_path,
        total_elapsed,
    )
    return index


def _load_pref_city_index_background() -> None:
    global _pref_city_index, _pref_city_loading, _pref_city_last_error  # pylint: disable=global-statement
    try:
        data = _load_pref_city_index()
        with _pref_city_lock:
            _pref_city_index = data
            _pref_city_last_error = None
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Failed to load pref_city dataset asynchronously: %s", exc)
        with _pref_city_lock:
            _pref_city_index = None
            _pref_city_last_error = str(exc)
    finally:
        with _pref_city_lock:
            _pref_city_loading = False
        _pref_city_load_event.set()


def _start_pref_city_load(force: bool = False) -> None:
    global _pref_city_index, _pref_city_loading, _pref_city_last_error  # pylint: disable=global-statement
    with _pref_city_lock:
        if not PREF_CITY_ENABLED:
            _pref_city_index = []
            _pref_city_loading = False
            _pref_city_last_error = None
            _pref_city_load_event.set()
            return
        if force:
            _pref_city_index = None
        if _pref_city_index is not None:
            _pref_city_load_event.set()
            return
        if _pref_city_loading:
            return
        _pref_city_loading = True
        _pref_city_last_error = None
        _pref_city_load_event.clear()
        thread = threading.Thread(target=_load_pref_city_index_background, name="pref-city-loader", daemon=True)
        thread.start()


def _get_pref_city_index(wait: bool = True) -> List[Dict[str, Any]]:
    global _pref_city_index  # pylint: disable=global-statement

    if _pref_city_index is not None:
        return _pref_city_index

    _start_pref_city_load()
    if wait:
        _pref_city_load_event.wait()
    return _pref_city_index or []


def get_pref_city_status() -> Dict[str, Any]:
    with _pref_city_lock:
        entry_count = len(_pref_city_index or [])
        if not PREF_CITY_ENABLED:
            return {
                "state": "disabled",
                "loaded": False,
                "entryCount": entry_count,
                "error": None,
            }
        if _pref_city_loading:
            state = "running"
            error = None
        elif _pref_city_last_error:
            state = "failed"
            error = _pref_city_last_error
        elif _pref_city_index is not None and entry_count > 0:
            state = "success"
            error = None
        elif _pref_city_index is not None and entry_count == 0:
            state = "success"
            error = None
        else:
            state = "idle"
            error = None
    return {
        "state": state,
        "loaded": state == "success",
        "entryCount": entry_count,
        "error": error,
    }


def start_pref_city_warmup(force: bool = False) -> Dict[str, Any]:
    _start_pref_city_load(force=force)
    return get_pref_city_status()


def _build_location_result(entry: Dict[str, Any], approximate: bool) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "prefecture": entry.get("prefecture"),
        "prefectureOffice": entry.get("prefectureOffice"),
        "municipality": entry.get("municipality"),
        "subMunicipality": entry.get("subMunicipality"),
        "cityCode": entry.get("cityCode"),
        "postalCode": None,
        "matchMethod": "pref_city_geojson",
    }
    if approximate:
        result["isApproximate"] = True

    label_parts = [entry.get("prefecture"), entry.get("municipality"), entry.get("subMunicipality")]
    label = " ".join(part for part in label_parts if part)
    if label:
        result["label"] = f"{label}*" if approximate else label
    return result


def _point_in_polygon_arrays(lon: float, lat: float, polygon: Tuple[array, array]) -> bool:
    xs, ys = polygon
    n = len(xs)
    if n < 3:
        return False
    inside = False
    j = n - 1
    for i in range(n):
        xi = xs[i]
        yi = ys[i]
        xj = xs[j]
        yj = ys[j]
        intersects = ((yi > lat) != (yj > lat)) and (
            lon < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi
        )
        if intersects:
            inside = not inside
        j = i
    return inside


def _lookup_pref_city_local(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    index = _get_pref_city_index(wait=False)
    if not index:
        return None

    for entry in index:
        minx, miny, maxx, maxy = entry["bbox"]
        if lon < minx or lon > maxx or lat < miny or lat > maxy:
            continue
        polygons: List[Tuple[array, array]] = entry["polygons"]
        for polygon in polygons:
            if _point_in_polygon_arrays(lon, lat, polygon):
                return _build_location_result(entry, approximate=False)

    nearest_entry: Optional[Dict[str, Any]] = None
    nearest_distance: float = float("inf")
    for entry in index:
        cx, cy = entry["centroid"]
        dist = (lon - cx) ** 2 + (lat - cy) ** 2
        if dist < nearest_distance:
            nearest_distance = dist
            nearest_entry = entry

    if nearest_entry is not None:
        return _build_location_result(nearest_entry, approximate=True)

    return None


_location_cache: Dict[Tuple[float, float], Dict[str, Any]] = {}


def lookup_pref_city(latitude: float, longitude: float) -> Optional[Dict[str, Any]]:
    if latitude is None or longitude is None:
        return None
    try:
        lat_raw = float(latitude)
        lon_raw = float(longitude)
    except (TypeError, ValueError):
        return None

    lat_raw, lon_raw = _normalize_lat_lon(lat_raw, lon_raw)
    lat = round(lat_raw, 5)
    lon = round(lon_raw, 5)
    cache_key = (lat, lon)
    cached = _location_cache.get(cache_key)
    if cached:
        return cached

    result = _lookup_pref_city_local(lat, lon)
    if result:
        _location_cache[cache_key] = result
    return result


def ensure_pref_city_loaded(wait: bool = True) -> Dict[str, Any]:
    """Ensure pref/city index is loaded into memory and report status."""
    _get_pref_city_index(wait=wait)
    return get_pref_city_status()


async def ensure_pref_city_loaded_async(wait: bool = True) -> Dict[str, Any]:
    """Async helper to avoid blocking the event loop when waiting for warmup."""
    return await asyncio.to_thread(ensure_pref_city_loaded, wait)


def _extract_geometry(boundary: Any) -> Optional[Dict[str, Any]]:
    if not boundary:
        return None

    if isinstance(boundary, dict):
        candidates = [
            boundary.get("geojson"),
            boundary.get("geoJson"),
            boundary.get("geometry"),
            boundary,
        ]
        for candidate in candidates:
            if isinstance(candidate, dict) and candidate.get("type") in {"Polygon", "MultiPolygon"}:
                if candidate.get("coordinates"):
                    return candidate
        return None

    if isinstance(boundary, str):
        text = boundary.strip()
        if text.startswith("{") and text.endswith("}"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return None
            return _extract_geometry(parsed)

    return None


def _centroid_from_geometry(geometry: Dict[str, Any]) -> Optional[Dict[str, float]]:
    geom_type = (geometry or {}).get("type")
    coordinates = (geometry or {}).get("coordinates")
    if not geom_type or not coordinates:
        return None

    total_weight = 0.0
    weighted_cx = 0.0
    weighted_cy = 0.0
    fallback_point: Optional[Tuple[float, float]] = None

    def _accumulate_ring(raw_ring: Any) -> None:
        nonlocal total_weight, weighted_cx, weighted_cy, fallback_point
        normalized: List[Tuple[float, float]] = []
        for pt in raw_ring or []:
            mapped = _normalize_point(pt)
            if mapped:
                normalized.append(mapped)
                if fallback_point is None:
                    fallback_point = mapped
        if len(normalized) < 3:
            return

        normalized = _ensure_closed_ring(normalized)
        area, cx, cy = _polygon_area_and_centroid(normalized)
        weight = abs(area) or 1e-9
        total_weight += weight
        weighted_cx += cx * weight
        weighted_cy += cy * weight

    if geom_type == "Polygon":
        if isinstance(coordinates, list) and coordinates:
            _accumulate_ring(coordinates[0])
    elif geom_type == "MultiPolygon":
        if isinstance(coordinates, list):
            for polygon in coordinates:
                if isinstance(polygon, list) and polygon:
                    _accumulate_ring(polygon[0])
    else:
        return None

    if total_weight == 0.0:
        if fallback_point is None:
            return None
        lon, lat = fallback_point
    else:
        lon = weighted_cx / total_weight
        lat = weighted_cy / total_weight

    lat, lon = _normalize_lat_lon(lat, lon)
    return {"latitude": lat, "longitude": lon}


def extract_field_centroid(field: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    direct_candidates = [
        field.get("centroid"),
        field.get("center"),
        field.get("centerPoint"),
    ]
    for candidate in direct_candidates:
        if isinstance(candidate, dict):
            lat = candidate.get("latitude") or candidate.get("lat")
            lon = candidate.get("longitude") or candidate.get("lon")
            if lat is not None and lon is not None:
                lat_f = float(lat)
                lon_f = float(lon)
                lat_f, lon_f = _normalize_lat_lon(lat_f, lon_f)
                return {"latitude": lat_f, "longitude": lon_f, "source": "direct"}

    lat_keys = ["latitude", "lat", "centroidLatitude"]
    lon_keys = ["longitude", "lon", "lng", "centroidLongitude"]
    lat = next((field.get(k) for k in lat_keys if field.get(k) is not None), None)
    lon = next((field.get(k) for k in lon_keys if field.get(k) is not None), None)
    if lat is not None and lon is not None:
        lat_f = float(lat)
        lon_f = float(lon)
        lat_f, lon_f = _normalize_lat_lon(lat_f, lon_f)
        return {"latitude": lat_f, "longitude": lon_f, "source": "direct"}

    for farm_key in ("farmV2", "farm"):
        farm = field.get(farm_key)
        if isinstance(farm, dict):
            lat = farm.get("latitude") or farm.get("lat")
            lon = farm.get("longitude") or farm.get("lon")
            if lat is not None and lon is not None:
                lat_f = float(lat)
                lon_f = float(lon)
                lat_f, lon_f = _normalize_lat_lon(lat_f, lon_f)
                return {"latitude": lat_f, "longitude": lon_f, "source": "farm"}

    boundary = field.get("boundary")
    geometry = _extract_geometry(boundary)
    if geometry:
        centroid = _centroid_from_geometry(geometry)
        if centroid:
            centroid["source"] = "boundary"
            return centroid
    return None


def enrich_field_with_location(field: Dict[str, Any]) -> None:
    centroid = extract_field_centroid(field)
    if not centroid:
        return

    location: Dict[str, Any] = {
        "center": {"latitude": centroid["latitude"], "longitude": centroid["longitude"]},
        "centerSource": centroid.get("source"),
    }

    pref_city = lookup_pref_city(centroid["latitude"], centroid["longitude"])
    if pref_city:
        location.update(pref_city)

    field["location"] = location
    # フロントエンド互換用に center/centroid も補完しておく
    existing_center = field.get("center")
    if not isinstance(existing_center, dict) or existing_center.get("latitude") in (None, "") or existing_center.get("longitude") in (None, ""):
        field["center"] = {"latitude": location["center"]["latitude"], "longitude": location["center"]["longitude"]}
    existing_centroid = field.get("centroid")
    if not isinstance(existing_centroid, dict) or existing_centroid.get("latitude") in (None, "") or existing_centroid.get("longitude") in (None, ""):
        field["centroid"] = {"latitude": location["center"]["latitude"], "longitude": location["center"]["longitude"]}
