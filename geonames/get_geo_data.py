import geonames
from bs4 import Tag
import time

def get_metadata(geoId):
    """Request metadata by Geonames ID, and return cleaned object"""

    params = {"db": "Taxonomy", "id": geoId}
    soup = ncbi.api_soup("efetch", params)

    taxon = soup.TaxaSet.Taxon

    taxon_metadata = {
        "ScientificName": taxon.ScientificName.getText(),
        "ParentTaxId": taxon.ParentTaxId.getText(),
        "Rank": taxon.Rank.getText(),
        "Division": taxon.Division.getText(),
        "GeneticCode": {"GCId": taxon.GCId.getText(), "GCName": taxon.GCName.getText()},
        "MitoGeneticCode": {
            "MGCId": taxon.MGCId.getText(),
            "MGCName": taxon.MGCName.getText(),
        },
        "Lineage": taxon.Lineage.getText(),
        "CreateDate": taxon.CreateDate.getText(),
        "UpdateDate": taxon.UpdateDate.getText(),
        "PubDate": taxon.PubDate.getText(),
    }

    if taxon.otherNames:
        taxon["OtherNames"] = (taxon.OtherNames.getText(),)

    # parse lineage
    lineage_ex = []
    for taxon in taxon.LineageEx.children:
        if isinstance(taxon, Tag):
            lineage_ex.append(
                {
                    "TaxId": taxon.TaxId.getText(),
                    "ScientificName": taxon.ScientificName.getText(),
                    "Rank": taxon.Rank.getText(),
                }
            )
        time.sleep(SLEEP_TIME)
    taxon_metadata["LineageEx"] = lineage_ex

    return taxon_metadata
