import datetime
import dlt
from requests import Request, Response
from typing import Any
from rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)

# from dlt.common import logger
from loguru import logger
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator


# TODO: is this implementation correct? Do I have to override any more methods?
class SocrataPaginator(OffsetPaginator):
    def update_state(self, response: Response) -> None:
        super().update_state(response)

        # In `response.request.url`, spaces are represented as `+`.
        # We need to convert them to `%20`.
        request_url = response.request.url.replace("+", "%20")

        items = response.json()

        if len(items) < self.limit:
            self._has_next_page = False
            logger.info(
                f"stop paginating: {request_url} returned less items than limit ({len(items)} < {self.limit})"
            )


num_rows_in_nyc_311_service_requests_dataset = 37_189_770  # checked on 2024/08/09
num_rows_in_nyc_film_permits_dataset = 7144  # checked on 2024/08/09
num_rows_in_nyc_staten_island_ferry_ridership_count = 2077  # checked on 2024/08/09

paginator_maximum_offset = max(
    num_rows_in_nyc_311_service_requests_dataset,
    num_rows_in_nyc_film_permits_dataset,
    num_rows_in_nyc_staten_island_ferry_ridership_count,
)

today = datetime.datetime.today()
thirty_days_ago = today - datetime.timedelta(days=30)


@dlt.source
def nyc_open_data_source(
    created_date_start: str = thirty_days_ago.strftime("%Y-%m-%d"),
    created_date_stop: str = today.strftime("%Y-%m-%d"),
    paginator_limit: int = 10_000,
    paginator_offset: int = 0,
    socrata_application_token: str = dlt.secrets.value,
) -> Any:
    """Fetch NYC data from the Socrata Open Data API.

    https://dev.socrata.com/
    """

    logger.info(
        {
            "created_date_start": created_date_start,
            "created_date_stop": created_date_stop,
            "paginator_limit": paginator_limit,
            "paginator_offset": paginator_offset,
            "paginator_maximum_offset": paginator_maximum_offset,
            # The Socrata application token is not a secret, so it's safe to log it.
            "socrata_application_token": socrata_application_token,
        }
    )

    paginator = SocrataPaginator(
        limit=paginator_limit,
        limit_param="$limit",
        offset=paginator_offset,
        offset_param="$offset",
        # Unfortunately, JSON responses from the Socrata Open Data API do not
        # include a total count, so we don't know for sure when to stop paginating.
        total_path=None,
        # Since we don't know when to stop paginating, we need to stop the paginator
        # when it has fetched some number of JSON resourses from the API.
        # In theory, one could set 2010-01-01 as the start date, and today's date as
        # the stop date. If we want to allow that, we need to set `maximum_offset`
        # to the total number of records in the NYC 311 Service Requests dataset.
        maximum_offset=paginator_maximum_offset,
    )

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://data.cityofnewyork.us/resource/",
            "headers": {
                # https://dev.socrata.com/docs/app-tokens
                "X-App-Token": socrata_application_token
            },
            "paginator": paginator,
        },
        "resource_defaults": {
            "write_disposition": "merge",
            "endpoint": {
                "params": {
                    "$limit": 100,
                    "$offset": 0,
                },
            },
        },
        "resources": [
            # https://dev.socrata.com/foundry/data.cityofnewyork.us/erm2-nwe9
            # https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/about_data
            {
                "name": "service_requests_311",
                "primary_key": "unique_key",
                "endpoint": {
                    "path": "erm2-nwe9.json",
                    "params": {
                        "$order": "created_date",
                        # https://dev.socrata.com/docs/functions/between
                        "$where": f"created_date between '{created_date_start}' and '{created_date_stop}'",
                    },
                },
            },
            # https://dev.socrata.com/foundry/data.cityofnewyork.us/tg4x-b46p
            # https://data.cityofnewyork.us/City-Government/Film-Permits/tg4x-b46p/about_data
            {
                "name": "film_permits",
                "primary_key": "eventid",
                "endpoint": {
                    "path": "tg4x-b46p.json",
                    "params": {
                        "$order": "eventid",
                    },
                },
            },
            # https://dev.socrata.com/foundry/data.cityofnewyork.us/6eng-46dm
            # https://data.cityofnewyork.us/Transportation/Staten-Island-Ferry-Ridership-Counts/6eng-46dm/about_data
            {
                "name": "staten_island_ferry_ridership_counts",
                "merge_key": "date",
                "endpoint": {
                    "path": "6eng-46dm.json",
                    "params": {
                        "$order": "date",
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)
