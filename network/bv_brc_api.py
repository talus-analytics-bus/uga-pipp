import aiohttp
from loguru import logger


class BVBRCApi:
    async def retrieve_data(self, data_type: str, count: int = 100) -> list:
        results = []
        counter = 0
        empty = False

        while not empty:
            result = self._bv_brc_api(data_type=data_type, count=count, start=counter)

            if not result:
                empty = True

            results.extend(result)
            counter += count

        return results

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
