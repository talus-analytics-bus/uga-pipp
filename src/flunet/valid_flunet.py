import csv
from datetime import datetime


FIELDNAMES = (
    "reportId",
    "Territory",
    "WHO region",
    "Transmission zone",
    "Year",
    "Week",
    "startDate",
    "endDate",
    "Collected",
    "Processed",
    "AH1",
    "AH1N1",
    "AH3",
    "AH5",
    "Anotsubtyped",
    "Atotal",
    "BYamagata",
    "BVictoria",
    "Bnotsubtyped",
    "Btotal",
    "caseCount",
    "Total negative",
    "ILI activity",
)


def is_valid_report(data: dict) -> bool:
    counts = [
        data["Collected"],
        data["Processed"],
        data["caseCount"],
        data["Total negative"],
    ]
    counts = [x in ["", "0"] for x in counts]

    if all(counts):
        return False
    return True


def process_dates(data: dict) -> None:
    data["startDate"] = datetime.strptime(data["startDate"], "%m/%d/%y").strftime(
        "%Y-%m-%d"
    )
    data["endDate"] = datetime.strptime(data["endDate"], "%m/%d/%y").strftime(
        "%Y-%m-%d"
    )


def valid_flunet(geo_api, file: str = "data/flunet_1995_2022.csv") -> list[dict]:
    flunet_valid = list()
    geonames = set()
    geoname_ids = []

    with open(file, "r", encoding="utf-8") as flunet_file:
        flunet = csv.DictReader(flunet_file)
        flunet.fieldnames = FIELDNAMES
        next(flunet)
        for row in flunet:
            if not is_valid_report(row):
                continue

            process_dates(row)

            row["reportId"] = {row['reportId']}
            row["eventId"] = f"Flu-{row['Territory']}-{str(row['startDate'])}"

            flunet_valid.append(row)

            # Process territories
            row["Territory"] = territory = (
                row["Territory"].lower().strip().replace('"', "")
            )
            if territory not in geonames and territory is not None:
                geoname_ids.append(geo_api.search_geoname_id(territory))

            geonames.add(territory)

    return flunet_valid, geonames, geoname_ids
