from datetime import datetime
from src.wahis.wahis_api import WAHISApi
from network.handle_concurrency import handle_concurrency


def process_dates(date: str) -> str:
    if not date:
        return None

    date_strip = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f%z")
    return date_strip.strftime("%Y-%m-%d")


def remove_unneded_keys(row: dict) -> None:
    row.pop("sources", None)
    row.pop("measures", None)
    row.pop("methods", None)
    row.pop("strategy", None)
    row.pop("laboratoryTests", None)
    row.pop("quantitativeData", None)
    row.pop("selfDeclaration", None)


def process_report(metadata: dict, tax_names: set, lat_long: set):
    metadata["report"]["uqReportId"] = f"WAHIS-{metadata['report']['reportId']}"
    metadata["report"]["reportedOn"] = process_dates(metadata["report"]["reportedOn"])

    if metadata["event"]["eventComment"]:
        metadata["event"]["eventComment"] = (
            metadata["event"]["eventComment"]["translation"]
            or metadata["event"]["eventComment"]["original"]
        )

    tax_names.add(metadata["event"]["disease"]["group"])
    tax_names.add(metadata["event"]["causalAgent"]["name"])

    metadata["quantitativeData"]["totals"] = metadata["quantitativeData"]["totals"][0]

    for outbreak in metadata["outbreaks"]:
        location = (outbreak["latitude"], outbreak["longitude"])
        outbreak["geonames"] = location
        lat_long.add(location)

        outbreak["startDate"] = process_dates(outbreak["startDate"])
        outbreak["endDate"] = process_dates(outbreak["endDate"])

    remove_unneded_keys(metadata)
    return metadata


def is_valid(row: dict, empty: tuple = (None, "")) -> bool:
    if row["event"]["disease"]["group"] in empty:
        return False

    if row["event"]["causalAgent"]["name"] in empty:
        return False

    return True


async def valid_wahis(geoapi, ncbiapi, wahis=WAHISApi()) -> list:
    lat_long = set()
    tax_names = set()

    evolutions = await handle_concurrency(
        *[wahis.search_evolution(event_id) for event_id in range(4714, 5097)]
    )

    reports = await handle_concurrency(
        *[
            wahis.search_report(report["reportId"])
            for evolution in evolutions
            if evolution is not None
            for report in evolution
        ]
    )

    wahis_valid = [process_report(report, tax_names, lat_long) for report in reports]

    geoname_ids = await handle_concurrency(
        *[geoapi.search_lat_long(location) for location in lat_long]
    )

    tax_ids = await handle_concurrency(
        *[ncbiapi.search_id(tax) for tax in tax_names], n_semaphore=2
    )
    geonames = dict(zip(lat_long, geoname_ids))

    wahis_valid = [x for x in wahis_valid if is_valid(x)]

    return wahis_valid, geonames, tax_names, tax_ids
