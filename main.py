import os
import time

from neo4j import GraphDatabase
from dotenv import load_dotenv

import carnivoreGMPD
import ncbi

load_dotenv()
# pull env vars for auth and create neo4j driver
NEO4J_AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
SESSION = NEO4J_DRIVER.session()

def db_merge_gmpd_ncbi(host_species, pathogen_species):
    try:
        host_ncbi_id = ncbi.id_search(f"{host_species}")
        if host_ncbi_id:
            host_ncbi_metadata = ncbi.get_taxon(host_ncbi_id)
            host_taxon = {**host_ncbi_metadata, "TaxId": host_ncbi_id}
            ncbi.merge_taxon(host_taxon, SESSION)
            time.sleep(0.4) 

    except Exception as e:
        with open("not_found.txt", "a") as f:
            f.write(f"Error getting taxon for host {host_species}: {e}\n")

    try:
        pathogen_ncbi_id = ncbi.id_search(f"{pathogen_species}")
        if pathogen_ncbi_id:
            pathogen_ncbi_metadata = ncbi.get_taxon(pathogen_ncbi_id)
            pathogen_taxon = {**pathogen_ncbi_metadata, "TaxId": pathogen_ncbi_id}
            ncbi.merge_taxon(pathogen_taxon, SESSION)
            time.sleep(0.4)
        
    except Exception as e:
        with open("not_found.txt", "a") as f:
             f.write(f"Error getting taxon for pathogen {pathogen_species}: {e}\n")           

if __name__ == "__main__":
    gmpd_rows = carnivoreGMPD.get_rows()

    for row in gmpd_rows:
        host_species = row['host_species']
        pathogen_species = row['pathogen_species']

        # Iterate over dataset and create separate taxon nodes for hosts and pathogens (with labels)
        host_node = SESSION.run(
            'MERGE (n:Taxon:Host:Species {name: $species}) '
            'SET n.Rank = "Species" '
            'RETURN n',
            species=host_species
        ).single().get('n')

        pathogen_node = SESSION.run(
            'MERGE (n:Taxon:Pathogen:Species {name: $species}) '
            'SET n.Rank = "Species", n.closeTransmission = $closeTransmission, n.noncloseTransmission = $noncloseTransmission, n.vectorTransmission = $vectorTransmission, n.intermediateTransmission = $intermediateTransmission '
            'RETURN n',
            species=pathogen_species,
            closeTransmission=row["close"],
            noncloseTransmission=row["nonclose"],
            vectorTransmission=row["vector"],
            intermediateTransmission = row["intermediate"]
        ).single().get("n")

    # SESSION.run(
    #     f'MERGE (n:Taxon:{rank} {{name: "{taxon["ScientificName"]}", '
    #     f'  Rank: "{rank}", '
    #     f'  TaxId: "{taxon["TaxId"]}" '
    #     f"}})"

        # Create INFECTS relationship between hosts and pathogens
        SESSION.run(
            'MATCH (h:Taxon:Host {name: $host_species}), (p:Taxon:Pathogen {name: $pathogen_species}) '
            'MERGE (p)-[:INFECTS]->(h)',
            host_species=host_species,
            pathogen_species=pathogen_species
        )

        # Merge host and pathogen nodes with NCBI metadata
        db_merge_gmpd_ncbi(host_species, pathogen_species)

    NEO4J_DRIVER.close()
