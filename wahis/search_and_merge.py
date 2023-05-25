import pickle
import os
import ncbi

WAHIS_NOT_FOUND = "wahis/species_not_found.txt"
wahis_searched = set()
wahis_not_found = set()

# Path to the pickle cache file
WAHIS_CACHE = "wahis/wahis_cache.pickle"

# Load the cache from the pickle file if it exists
wahis_cache = {}
if os.path.exists(WAHIS_CACHE):
    with open(WAHIS_CACHE, "rb") as f:
        wahis_cache = pickle.load(f)

# Function to save the cache to the pickle file
def save_cache():
    with open(WAHIS_CACHE, "wb") as f:
        pickle.dump(wahis_cache, f)

def write_to_not_found(term):
    global wahis_not_found
    if term not in wahis_not_found:
        with open(WAHIS_NOT_FOUND, "a") as f:
            f.write(term)
        wahis_not_found.add(term)

def search_and_merge(term, SESSION):
    global wahis_cache, wahis_searched, wahis_not_found

    if term in wahis_cache:
        taxon = wahis_cache[term]
        ncbi.merge_taxon(taxon, SESSION)
        ncbi_id = taxon["taxId"]

    else:
        wahis_searched.add(term)
        ncbi_id = ncbi.id_search(term)
        if ncbi_id:
            ncbi_metadata = ncbi.get_metadata(ncbi_id)
            taxon = {**ncbi_metadata, "taxId": ncbi_id}
            ncbi.merge_taxon(taxon, SESSION)
            wahis_cache[term] = taxon
            save_cache()
        else:
            wahis_not_found.add(term)
            return None

    return ncbi_id