import os
import asyncio
from neo4j import AsyncGraphDatabase


NEO4J_URI = os.environ["NEO4J_URI"]
NEO4J_USER = os.environ["NEO4J_USER"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
NEO4J_DATABASE = os.environ["NEO4J_DATABASE"]


class Neo4jHandler:
    def __init__(
        self,
        uri: str = NEO4J_URI,
        user: str = NEO4J_USER,
        password: str = NEO4J_PASSWORD,
        database: str = NEO4J_DATABASE,
    ) -> None:
        self.driver = AsyncGraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    async def execute_query(self, query_file: str, properties: dict) -> bool:
        with open(query_file, "r", encoding="utf-8") as file:
            query = file.read()

            async with self.driver.session(database=self.database) as session:
                await session.run(query, properties)
