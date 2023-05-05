import os
from dotenv import load_dotenv
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
        flunet_to_ncbi[key] = value


def create_query_line_data(row_number: int) -> str:
    # Query node by row number, return node and first order relationships including nodes
    # The query should match the row data
    query = f"MATCH(n)-[r]-(b) WHERE n:FluNet AND n:CaseReport AND n.dataSourceRow = {row_number} RETURN n, r, b, type(r)"
    return query


def is_flunet_node_accurate(row: dict, node: dict) -> bool:
    keys = [
        "",
        "Start date",
        "End date",
        "Collected",
        "Processed",
        "Total positive",
        "Total negative",
    ]

    node_dictionary = [row[key] for key in keys]

    return collections.Counter(node_dictionary) == collections.Counter(
        list(node.values())
    )


def test_flunet_line_data(csv_row: dict, query_results: list) -> bool:
    # Verify node exists
    accuracy = {}
    is_node_checked = False
    for result in query_results:
        node, relationship, adjacent_node, type_relationship = result.values()
        # Check the primary node
        if not is_node_checked:
            node_accuracy = is_flunet_node_accurate(csv_row, node)
            accuracy["node"] = node_accuracy
            is_node_checked = True
if __name__ == "__main__":

    with open("./flunet/data/flunet_1995_2022.csv", "r") as flunet:
        header = next(flunet)  # remove header
        for row in flunet:
            query = create_query_line_data