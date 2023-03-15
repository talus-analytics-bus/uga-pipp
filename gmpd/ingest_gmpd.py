import gmpd
from loguru import logger
from datetime import datetime

def ingest_gmpd(SESSION):
    gmpd_rows = gmpd.get_rows()

    # Make sure taxons exist in the database
    # Cast variables for properties
    for row_id, row in enumerate(gmpd_rows):

        citation = row["Citation"]
        prevalence = row["Prevalence"]
        collected = row["HostsSampled"]
        sampleType = row["SamplingType"]        

        # Create the Report node if it doesn't exist, and set its label to GMPD
        query = """
        MERGE (r:GMPD:Report {citation:$citation, prevalence:$prevalence, collected:$collected, sampleType:$sampleType, row_id:$row_id})
        RETURN r
        """
        parameters = {"citation": citation, "prevalence": prevalence, "collected": collected, "sampleType": sampleType, "row_id": row_id}
        result = SESSION.run(query, parameters)
        report_node = result.single()[0]

        host_ncbi_id, pathogen_ncbi_id = gmpd.link_gmpd_to_ncbi(row, SESSION)

        if host_ncbi_id:

            # Create the relationships between the Report node and the host taxon
            query = """
            MATCH (r:GMPD:Report {row_id: $row_id}), (h:Taxon {TaxId: $host_ncbi_id})
            MERGE (r)-[hr:REPORTS {host: 1}]->(h)
            """
            parameters = {"row_id": row_id,"host_ncbi_id": host_ncbi_id}
            result = SESSION.run(query, parameters)

        if pathogen_ncbi_id:

            # Create the relationships between the Report node and the pathogen taxon
            query = """
            MATCH (r:GMPD:Report {row_id: $row_id}), (p:Taxon {TaxId: $pathogen_ncbi_id})
            MERGE (r)-[pr:REPORTS {pathogen: 1}]->(p)
            """
            parameters = {"row_id": row_id, "pathogen_ncbi_id": pathogen_ncbi_id}
            result = SESSION.run(query, parameters)

