import aiohttp
from loguru import logger


class BVBRC:
    async def _bv_brc_api(self):
        base_url = "https://patricbrc.org/api/"
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(base_url) as response:
                return await response.json()
