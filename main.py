import pickle
import sys
import asyncio
from loguru import logger
from network.neo4j_handler import Neo4jHandler
from network.geo_api import GeonamesApi
from network.ncbi_api import NCBIApi

from src.flunet.ingest_flunet import ingest_flunet
from src.gmpd.ingest_gmpd import ingest_gmpd
from src.wahis.ingest_wahis import ingest_wahis
from src.combine.ingest_combine import ingest_combine
from src.virion.ingest_virion import ingest_virion
from src.worldpop.ingest_worldpop import ingest_worldpop

# from src.bvbrc_surveillance.ingest_bvbrc_surveillance import ingest_bvbrc_surveillance


logger.remove(0)
logger.add(sys.stderr, level="TRACE")


async def main() -> None:
    database_handler, geonames_api, ncbi_api = Neo4jHandler(), GeonamesApi(), NCBIApi()

    ## Code to drop NONE out of the ncbi ID cache
    ## so that we can use an updated synonyms map

    # new_cache = {}
    # with open("network/cache/ncbi_id.pickle", "rb") as c:
    #     ncbi_cache = pickle.load(c)
    #     for key, value in ncbi_cache.items():
    #         if value is not None:
    #             new_cache[key] = value
    # with open("network/cache/ncbi_id.pickle", "wb") as c:
    #     pickle.dump(new_cache, c)

    await ingest_wahis(database_handler, geonames_api, ncbi_api)
    await ingest_virion(database_handler, ncbi_api)
    await ingest_flunet(database_handler, geonames_api, ncbi_api)
    await ingest_gmpd(database_handler, geonames_api, ncbi_api)

    # await ingest_bvbrc_surveillance(database_handler, geonames_api, ncbi_api)

    # Keep combine and worldpop at the end of ingestion
    await ingest_combine(database_handler)
    await ingest_worldpop(database_handler, geonames_api)


if __name__ == "__main__":
    asyncio.run(main())
