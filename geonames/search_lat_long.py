from geonames import geo_api
from loguru import logger
from functools import cache

@cache
def search_lat_long(lat, long):
    # Use the lat, long to find nearest place
    params = {"lat": lat, "lng": long}
    result = geo_api("findNearbyJSON", params)

    # Check if any results were found
    if not result.get("geonames"):
        logger.warning(f"No nearby places found for lat: {lat}, long: {long}")
        return None

    geonameId = result["geonames"][0]["geonameId"]

    # Return the name of the nearest place
    return geonameId