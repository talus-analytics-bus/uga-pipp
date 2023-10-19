import aiohttp
from aiohttp import ContentTypeError


class WAHISApi:
    async def search_evolution(self, event_id):
        evolution_url = f"event/{event_id}/report-evolution?language=en"
        return await self._wahis_api(evolution_url)

    async def search_outbreak(self, report_id, event_id):
        outbreak_url = (
            f"review/report/{report_id}/outbreak/{event_id}/all-information?language=en"
        )
        return await self._wahis_api(outbreak_url)

    async def search_report(self, report_id):
        report_url = f"review/report/{report_id}/all-information?language=en"
        return await self._wahis_api(report_url)

    async def _wahis_api(self, url) -> dict:
        base_url = f"https://wahis.woah.org/api/v1/pi/{url}"
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(base_url) as response:
                try:
                    response.json()
                except ContentTypeError:
                    return None
