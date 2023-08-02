import re
from datetime import datetime
from loguru import logger


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


def process_header(header: str) -> list:
    logger.info("Updating header")
    header = header.strip().split(",")
    header = ["startDate" if item == "Start date" else item for item in header]
    header = ["endDate" if item == "End date" else item for item in header]
    header = ["caseCount" if item == "Total positive" else item for item in header]
    header = ["reportId" if item == "" else item for item in header]
    return header


def process_dates(data: dict) -> None:
    data["startDate"] = datetime.strptime(data["startDate"], "%m/%d/%y").strftime(
        "%Y-%m-%d"
    )
    data["endDate"] = datetime.strptime(data["endDate"], "%m/%d/%y").strftime(
        "%Y-%m-%d"
    )


def extract_row(string: str) -> list:
    row_data = re.findall(r"(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", string)
    if "(" in row_data[1]:
        row_data[1] = re.match(r"([A-Za-z]*)\s(?=\(.*\))", row_data[1]).group(0)
        return row_data
    if "," in row_data[1]:
        row_data[1] = row_data[1].split(",")[1]
        return row_data
    return row_data


def split_influenza_type(valid: list[dict]) -> tuple[list[dict]]:
    logger.info("Splitting influenza types")
    influenza_a = [data for data in valid if data["A (total)"] not in ["", "0"]]
    influenza_a = [dict(data, **{"type": "Influenza A"}) for data in influenza_a]
    influenza_b = [data for data in valid if data["B (total)"] not in ["", "0"]]
    influenza_b = [dict(data, **{"type": "Influenza B"}) for data in influenza_b]
    return influenza_a, influenza_b


def valid_flunet(file: str = "flunet/data/flunet_1995_2022.csv") -> list[dict]:
    logger.info("Validating flunet data")
    with open(file, "r", encoding="latin-1") as flunet:
        flunet_valid = []
        header = next(flunet)
        header = process_header(header)
        for row in flunet:
            row = extract_row(row.strip())
            data = dict(zip(header, row))
            if is_valid_report(data):
                process_dates(data)
                data["reportId"] = "FluNet" + "-" + data["reportId"]
                flunet_valid.append(data)
    return split_influenza_type(flunet_valid)
