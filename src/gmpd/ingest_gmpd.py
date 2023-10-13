import asyncio
from loguru import logger
from network.sempahore import handle_concurrency
from gmpd.valid_gmpd import valid_gmpd

QUERY = "./src/gmpd/gmpd.cypher"


async def ingest_gmpd(
    database_handler, geoapi, ncbiapi, batch_size: int = 1000, query_path=QUERY
) -> None:
    gmpd = valid_gmpd()
