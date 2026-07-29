import functools
import json
import struct
import threading
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ATTRIBUTION = "Bayerische Vermessungsverwaltung – www.geodaten.bayern.de"
TERRAIN_MEDIA_TYPE = "application/vnd.quantized-mesh"
GZIP_SIGNATURE = b"\x1f\x8b\x08"


def is_gzip_encoded(data: bytes) -> bool:
    """Return whether bytes start with the gzip magic and deflate method."""
    return data.startswith(GZIP_SIGNATURE)


def inspect_quantized_mesh(path: Path) -> dict:
    """Parse enough of a quantized-mesh tile to validate its structure and extensions."""
    data = path.read_bytes()
    if is_gzip_encoded(data):
        raise ValueError(f"{path} is gzip-compressed")
    if len(data) < 92:
        raise ValueError(f"{path} is too small to contain a quantized-mesh header")

    offset = 88
    vertex_count = _read_uint32(data, offset, path)
    offset += 4 + (vertex_count * 3 * 2)

    index_width = 4 if vertex_count > 65536 else 2
    offset = _align(offset, index_width)
    triangle_count = _read_uint32(data, offset, path)
    offset += 4 + (triangle_count * 3 * index_width)

    edge_vertex_counts = {}
    for edge in ("west", "south", "east", "north"):
        count = _read_uint32(data, offset, path)
        offset += 4 + (count * index_width)
        if offset > len(data):
            raise ValueError(f"{path} has a truncated {edge} edge index list")
        edge_vertex_counts[edge] = count

    extensions = []
    while offset < len(data):
        if len(data) - offset < 5:
            raise ValueError(f"{path} has a truncated extension header")
        extension_id = data[offset]
        extension_length = _read_uint32(data, offset + 1, path)
        offset += 5
        if offset + extension_length > len(data):
            raise ValueError(f"{path} has a truncated extension {extension_id}")
        extensions.append(extension_id)
        offset += extension_length

    if vertex_count == 0 or triangle_count == 0:
        raise ValueError(f"{path} has no terrain geometry")

    return {
        "bytes": len(data),
        "vertex_count": vertex_count,
        "triangle_count": triangle_count,
        "edge_vertex_counts": edge_vertex_counts,
        "extension_ids": extensions,
    }


def validate_tileset(terrain_dir: Path) -> dict:
    layer_path = terrain_dir / "layer.json"
    if not layer_path.is_file():
        raise FileNotFoundError(f"Missing terrain metadata: {layer_path}")

    with layer_path.open(encoding="utf-8") as file:
        layer = json.load(file)
    _validate_layer(layer)

    by_zoom: dict[int, list[Path]] = {}
    for path in terrain_dir.rglob("*.terrain"):
        relative = path.relative_to(terrain_dir)
        if len(relative.parts) != 3:
            raise ValueError(f"Unexpected terrain tile path: {relative}")
        z_text, x_text, filename = relative.parts
        y_text = filename.removesuffix(".terrain")
        if not (z_text.isdigit() and x_text.isdigit() and y_text.isdigit()):
            raise ValueError(f"Invalid terrain tile coordinates: {relative}")
        zoom = int(z_text)
        if path.stat().st_size == 0:
            raise ValueError(f"Empty terrain tile: {relative}")
        with path.open("rb") as file:
            if is_gzip_encoded(file.read(len(GZIP_SIGNATURE))):
                raise ValueError(f"Compressed terrain tile remains: {relative}")
        by_zoom.setdefault(zoom, []).append(path)

    expected_zooms = set(range(19))
    missing_zooms = sorted(expected_zooms - set(by_zoom))
    if missing_zooms:
        raise ValueError(f"Terrain output is missing zoom levels: {missing_zooms}")

    representative_zooms = (0, 9, 18)
    representatives = {}
    for zoom in representative_zooms:
        path = sorted(by_zoom[zoom])[len(by_zoom[zoom]) // 2]
        details = inspect_quantized_mesh(path)
        if 1 not in details["extension_ids"]:
            raise ValueError(f"{path} has no oct-encoded vertex-normal extension")
        representatives[str(zoom)] = {
            "tile": str(path.relative_to(terrain_dir)),
            **details,
        }

    http_smoke = _cesium_http_smoke(terrain_dir, layer, representatives["0"]["tile"])
    return {
        "format": layer["format"],
        "scheme": layer["scheme"],
        "minzoom": layer["minzoom"],
        "maxzoom": layer["maxzoom"],
        "tile_count": sum(len(paths) for paths in by_zoom.values()),
        "tiles_by_zoom": {
            str(zoom): len(by_zoom[zoom])
            for zoom in sorted(by_zoom)
        },
        "representative_tiles": representatives,
        "cesium_http_smoke": http_smoke,
    }


def _validate_layer(layer: dict) -> None:
    expected = {
        "format": "quantized-mesh-1.0",
        "scheme": "tms",
        "projection": "EPSG:4326",
        "minzoom": 0,
        "maxzoom": 18,
    }
    for key, value in expected.items():
        if layer.get(key) != value:
            raise ValueError(f"layer.json {key!r} must be {value!r}, got {layer.get(key)!r}")
    if "octvertexnormals" not in layer.get("extensions", []):
        raise ValueError("layer.json does not advertise oct-encoded vertex normals")
    tiles = layer.get("tiles")
    if not isinstance(tiles, list) or not tiles or "{z}/{x}/{y}.terrain" not in tiles[0]:
        raise ValueError("layer.json has no Cesium quantized-mesh tile template")
    if layer.get("attribution") != ATTRIBUTION:
        raise ValueError("layer.json is missing the required source-data attribution")
    if not layer.get("available") or len(layer["available"]) < 19:
        raise ValueError("layer.json availability does not cover zoom levels 0 through 18")


def _cesium_http_smoke(terrain_dir: Path, layer: dict, tile_relative: str) -> dict:
    handler = functools.partial(_QuietHandler, directory=str(terrain_dir))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}/"
        with urllib.request.urlopen(f"{base_url}layer.json", timeout=10) as response:
            served_layer = json.load(response)
        if served_layer.get("format") != "quantized-mesh-1.0":
            raise ValueError("HTTP-served layer.json is not quantized-mesh-1.0")

        request = urllib.request.Request(
            f"{base_url}{tile_relative}",
            headers={"Accept": f"{TERRAIN_MEDIA_TYPE};extensions=octvertexnormals"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            tile_bytes = response.read()
            content_encoding = response.headers.get("Content-Encoding")
        if content_encoding:
            raise ValueError(f"Terrain HTTP smoke test received Content-Encoding: {content_encoding}")
        if is_gzip_encoded(tile_bytes):
            raise ValueError("Terrain HTTP smoke test received gzip-compressed bytes")
        if len(tile_bytes) < 92:
            raise ValueError("Terrain HTTP smoke test received a truncated tile")
        return {
            "provider_url": base_url,
            "layer_loaded": True,
            "tile": tile_relative,
            "tile_bytes": len(tile_bytes),
            "requested_extensions": ["octvertexnormals"],
        }
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


class _QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, _format, *args):
        return


def _read_uint32(data: bytes, offset: int, path: Path) -> int:
    if offset + 4 > len(data):
        raise ValueError(f"{path} is truncated at byte {offset}")
    return struct.unpack_from("<I", data, offset)[0]


def _align(value: int, alignment: int) -> int:
    return (value + alignment - 1) // alignment * alignment
