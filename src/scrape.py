import hashlib
import os
import typing as t
from pathlib import Path
from urllib.parse import urljoin

import dlt
import pandas as pd
import requests
import tenacity
from dlt.common.typing import TDataItem
from dlt.sources.helpers.transform import add_row_hash_to_table
from loguru import logger

CLERK_BASE_URL = "https://2e6zstq3xm5z3sh4thbnvghddm0lzgeg.lambda-url.eu-central-1.on.aws"  # Ideally, we can configure this variable per environment and call a different endpoint when in prod vs. dev

# TODO: Get them from a HubSpot list
contacts = pd.read_csv(Path(__file__).parent / "static" / "contacts.csv").filter(
    ["LinkedIn Profile URL", "First Name", "Last Name"]
)


def add_row_hash_to_table(row_hash_column_name: str) -> TDataItem:
    """
    We scrape profile data to understand when people change their jobs.
    Hence, all that makes record "unique" is the combination of company + job title
    """

    # see: https://dlthub.com/docs/general-usage/incremental-loading#scd2-strategy
    def hash_company_name_job_description(table: TDataItem) -> TDataItem:
        table[row_hash_column_name] = hashlib.md5(
            f"{table['company'], table['job_title']}".encode("utf-8")
        ).hexdigest()

        return table

    return hash_company_name_job_description


@tenacity.retry(
    stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True
)
def get_profile_data(
    profile_url: str, clerk_api_key: str
) -> t.Tuple[t.Dict[str, t.Any], bool]:
    """Just a retry wrapper around the actual API call"""
    res = requests.get(
        urljoin(base=CLERK_BASE_URL, url="v1/data/people"),
        headers={
            "Authorization": f"Bearer {clerk_api_key}",
            "cache-control": None,  # alternative: no-cache
        },
        params={"profile_url": profile_url},
    )
    if not res.ok:
        if res.status_code in (422, 404):
            # invalid profile URL or profile URL not found
            return {}, False
        raise Exception(
            f"Failed to pull profile data for {profile_url} "
            f"| status: {res.status_code} | {res.text}"
        )
    return res.json(), res.headers["x-cache"] == "HIT"


@dlt.resource(
    max_table_nesting=1,
    primary_key="profile_url",
    write_disposition={
        "disposition": "merge",
        "strategy": "scd2",
        "row_version_column_name": "row_hash",
    },
)
def personal_profile(
    clerk_api_key: str = dlt.secrets.value,
) -> t.Generator[t.Dict[str, t.Any], None, None]:
    """
    DLT resource to scrape linkedin profile data
    from self-hosted scraping API (clerk API).
    The API is just a cached wrapper around a commercial
    scraping API
    """
    total_cost = 0.0
    for idx, (profile_url, first_name, last_name) in contacts.iterrows():
        logger.info(f"Scraping profile data for {profile_url}")
        profile_data, is_cache_hit = get_profile_data(
            profile_url=profile_url, clerk_api_key=clerk_api_key
        )
        if not is_cache_hit:
            total_cost += 0.015  # Current plan: PRO => 3k credits / month @ 45€
        if profile_data:
            yield {
                "profile_url": profile_url,
                **profile_data["personal_info"],
                "education": profile_data["education"],
                "work_experience": profile_data["work_experience"],
            }
        else:
            logger.warning(f"No profile data found for '{profile_url}'")
    logger.info(f"Total cost of scraping linkedin: {total_cost:.2f}€")


if __name__ == "__main__":
    # load_dotenv(
    #     override=True
    # )  # load dlt secrets, see https://github.com/theskumar/python-dotenv/issues/289
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = os.environ[
        "DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"
    ].replace("\\n", "\n")
    p = dlt.pipeline(
        pipeline_name="clerk",
        destination=(
            dlt.destinations.duckdb()
            if os.environ.get(
                "ENV", "PROD"
            )  # Ideally, the 'ENV' variable would be injected from tower CLI
            == "local"
            else dlt.destinations.bigquery()
        ),
        progress="log",
    )
    p.run(
        personal_profile.add_map(add_row_hash_to_table("row_hash")),
        dataset_name="linkedin_data",
    )  # limit to 10 for testing purposes
