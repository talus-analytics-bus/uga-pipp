UNWIND $Mapping as mapping
MERGE (tax:Taxon {taxId : mapping.taxId})
ON CREATE SET 
    tax.name = mapping.name,
    tax.rank = mapping.rank,
    tax.dataSource = mapping.dataSource
WITH COLLECT(tax) AS hierarchy
UNWIND RANGE(0, SIZE(hierarchy) - 2) as idx
WITH hierarchy[idx] AS h1, hierarchy[idx+1] AS h2
MERGE (h1)-[:CONTAINS_TAX]->(h2)
