import datetime as dt
import time
import typing as t

import requests
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

LINEAR_GRAPHQL_URL = "https://api.linear.app/graphql"


class RetryException(Exception):
    pass


@retry(
    stop=stop_after_attempt(5),
    # Wait 2^x * 2 second between each retry starting with 30 seconds, up to 2 minutes
    wait=wait_exponential(multiplier=2, min=30, max=120),
    retry=retry_if_exception_type(RetryException),
    reraise=True,
)
def fetch_linear_graphql_api(
    api_key: str, query: str, entity: str
) -> t.Dict[str, t.Any]:
    response = requests.post(
        LINEAR_GRAPHQL_URL,
        headers={"Content-Type": "application/json", "Authorization": api_key},
        json={"query": query},
    )
    if (
        response.status_code >= 400
        and response.status_code < 500
        and int(response.headers.get("x-ratelimit-requests-remaining", "-1")) == 0
    ):
        reset_timestamp_utc = int(response.headers["x-ratelimit-requests-reset"][:10])
        reset_wait_time_seconds = reset_timestamp_utc - int(time.time()) + 1
        logger.warning(
            f"status: {response.status_code} | "
            "We hit the Linear-API request rate limit of "
            f"{response.headers.get('x-ratelimit-requests-limit')} "
            "requests per hour. The rate limit will be re-set at "
            f"{dt.datetime.fromtimestamp(1710437938).isoformat()} UTC, "
            f"which is {reset_wait_time_seconds} seconds from now."
        )
        time.sleep(reset_wait_time_seconds)
        raise RetryException(response.text)

    if (
        response.status_code >= 400
        and response.status_code < 500
        and int(response.headers.get("x-ratelimit-complexity-remaining", "-1")) == 0
    ):
        reset_timestamp_utc = int(response.headers["x-ratelimit-complexity-reset"][:10])
        reset_wait_time_seconds = reset_timestamp_utc - int(time.time()) + 1
        logger.warning(
            f"status: {response.status_code} | "
            "We hit the Linear-API complexity rate limit of "
            f"{response.headers.get('x-ratelimit-complexity-limit')} "
            "requests per hour. The rate limit will be re-set at "
            f"{dt.datetime.fromtimestamp(1710437938).isoformat()} UTC, "
            f"which is {reset_wait_time_seconds} seconds from now."
        )
        time.sleep(reset_wait_time_seconds)
        raise RetryException(response.text)

    if response.status_code >= 500:
        # retry on linear-side server errors:
        raise RetryException(f"status: {response.status_code} | {response.text}")

    if response.status_code != 200:
        logger.error(response.headers)
        raise Exception(f"status: {response.status_code} | {response.text}")

    raw_data = response.json()["data"][entity]
    sanitized_data = {str(k): v for k, v in raw_data.items()}

    return sanitized_data
