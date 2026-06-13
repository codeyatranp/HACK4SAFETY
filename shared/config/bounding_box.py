"""
Nepal geographic bounding box and coordinate validation utilities.
Used by all ingestion components for geo-validation.
"""

# Nepal bounding box (decimal degrees)
NEPAL_BOUNDS = {
    "north": 30.4,
    "south": 26.3,
    "east": 88.2,
    "west": 80.0,
}

# Approximate Nepal center point
NEPAL_CENTER = {"lat": 27.7172, "lng": 85.3240}


def in_nepal(lat: float, lng: float) -> bool:
    """
    Check if a coordinate falls within Nepal's approximate bounding box.
    Use for quick reject before detailed PostGIS validation.
    """
    return (
        NEPAL_BOUNDS["south"] <= lat <= NEPAL_BOUNDS["north"]
        and NEPAL_BOUNDS["west"] <= lng <= NEPAL_BOUNDS["east"]
    )


def crop_to_nepal(lat_points: list, lng_points: list) -> tuple[list, list]:
    """
    Filter coordinate pairs to those within Nepal's bounding box.
    Returns filtered (lat_list, lng_list).
    """
    filtered_lat = []
    filtered_lng = []
    for lat, lng in zip(lat_points, lng_points):
        if in_nepal(lat, lng):
            filtered_lat.append(lat)
            filtered_lng.append(lng)
    return filtered_lat, filtered_lng