import os
from dotenv import load_dotenv
import collections
from datetime import datetime
from neo4j_driver import Neo4jDatabase

load_dotenv()

URI = os.environ["URI"]
AUTH = os.environ["AUTH"]
PASSWORD = os.environ["PASSWORD"]
DATABASE = os.environ["DATABASE"]

neo4j_connection = Neo4jDatabase(URI, DATABASE, AUTH, PASSWORD)

flunet_to_ncbi = {}
with open("./flunet/data/flunet_to_ncbi.csv", "r") as flunet_ncbi:
    for record in flunet_ncbi:
        key, value = record.split(",")
        value = value.strip()
        flunet_to_ncbi[value] = key


def create_query_line_data(row_number: int) -> str:
    # Query node by row number, return node and first order relationships including nodes
    # The query should match the row data
    query = f"MATCH(n)-[r]-(b) WHERE n:FluNet AND n:CaseReport AND n.dataSourceRow = {row_number} RETURN n, r, b, type(r)"
    return query


def is_flunet_node_accurate(row: dict, node: dict) -> bool:
    keys = [
        "",
        "Start date",
        # "End date",
        "Collected",
        "Processed",
        "Total positive",
        "Total negative",
    ]

    node["start"] = node["start"].strftime("%m/%d/%y")

    node.pop("dataSource", None)  # Remove data source
    node.pop("duration", None)  # Remove duration
    node_dictionary = {key: row[key] for key in keys}
    node_dictionary["Start date"] = datetime.strptime(
        node_dictionary["Start date"], "%m/%d/%y"
    ).strftime("%m/%d/%y")
    # Empty strings to zero
    node_dictionary = {
        key: ("0" if value == "" else value) for key, value in node_dictionary.items()
    }
    node_dictionary_values = node_dictionary.values()
    node = [str(element) for element in node.values()]
    return collections.Counter(list(node_dictionary_values)) == collections.Counter(
        list(node)
    )


def is_line_null(line_data: dict) -> bool:
    keys = (
        "Collected",
        "Processed",
        "A (H1)",
        "A (H1N1)pdm09",
        "A (H3)",
        "A (H5)",
        "A (not subtyped)",
        "A (total)",
        "B (Yamagata)",
        "B (Victoria)",
        "B (not subtyped)",
        "B (total)",
        "Total positive",
        "Total negative",
    )

    return all(line_data[key] == "" for key in keys)


def is_strain_accurate(row: dict, strain_name: str) -> bool:
    strain = flunet_to_ncbi[strain_name]
    if row[strain] != 0:
        return True
    return False


def test_flunet_line_data(csv_row: dict, query_result: list) -> dict:
    # Verify node exists
    accuracy = {}
    is_node_checked = False
    for result in query_result:
        node, relationship, adjacent_node, type_relationship = result.values()
        # Check the primary node
        if not is_node_checked:
            node_accuracy = is_flunet_node_accurate(csv_row, dict(node))
            accuracy["node"] = node_accuracy
            is_node_checked = True
        # Check territorial scope
        if type_relationship == "IN":
            territory = csv_row["Territory"]
            territory_name = dict(adjacent_node)["name"]
            accuracy["territory"] = territory == territory_name
        # Check reported nodes
        if "host" not in relationship and type_relationship == "REPORTS":
            adj_name = adjacent_node["name"]
            adj_accuracy = is_strain_accurate(csv_row, adj_name)
            accuracy[adj_name] = adj_accuracy
    return accuracy


if __name__ == "__main__":

    with open("./flunet/data/flunet_1995_2022.csv", "r") as flunet:
        header = next(flunet).split(",")
        total = 0
        null = 0
        correct, incorrect = 0, 0
        for row in flunet:
            total += 1  # Count total number of rows
            row = row.split(",")
            # Create a dictionary with the line data
            row_as_dictionary = {k: v for k, v in zip(header, row)}

            if is_line_null(row_as_dictionary):
                null += 1  # Count empty rows
                continue

            query = create_query_line_data(row_as_dictionary[""])
            query_results = neo4j_connection.run_query(query)
            line_data_accuracy = test_flunet_line_data(row_as_dictionary, query_results)

            if all(line_data_accuracy.values()):
                correct += 1  # Count correct values
            else:
                with open("./validation/logs/flunet_validation.log", "a") as log_file:
                    incorrect += 1
                    msg = "ERROR: " + row_as_dictionary[""] + "\n"
                    log_file.write(msg)

            print(total, end="\r")
        print(
            "Total: ",
            total,
            "Empty: ",
            null,
            "Correct: ",
            correct,
            "Incorrect: ",
            incorrect,
        )
