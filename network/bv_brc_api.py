import aiohttp
from loguru import logger


class BVBRC:
    async def _bv_brc_api(self):
        base_url = "https://patricbrc.org/api/"
    async def _bv_brc_api(
        self,
        data_type,
        count: int,
        start: int,
    ):
        base_url = f"https://patricbrc.org/api/{data_type}/?http_content-range=99&http_accept=application/json&limit({count},{start})"
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(base_url) as response:
                return await response.json()
