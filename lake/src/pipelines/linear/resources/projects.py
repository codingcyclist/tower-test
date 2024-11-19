import datetime as dt
import typing as t

import dlt
from dateutil.parser import isoparse

from .utils import fetch_linear_graphql_api


def _fetch_linear_projects(
    api_key: str,
    date_lbound: dt.date,
    page_size: int = 100,
    next_cursor: t.Optional[str] = None,
) -> t.Dict[str, t.Any]:
    datetime_str = (  # 5 days overlap, just to be sure to catch all updates
        date_lbound - dt.timedelta(days=5)
    ).strftime("%Y-%m-%dT%H:%M:%S")
    query = f"""
        {{
            projects(
                first: {page_size}
                filter: {{
                    or: {{
                        createdAt: {{
                            gte: "{datetime_str}"
                        }},
                        updatedAt: {{
                            gte: "{datetime_str}"
                        }}
                    }}
                }}
                after: {'null' if next_cursor is None else '"' + next_cursor + '"'}
                includeArchived: true
            ) {{
                nodes {{
                    name
                    description
                    creator {{
                        name
                        email
                    }}
                    lead {{
                        name
                        email
                    }}
                    createdAt
                    updatedAt
                    archivedAt
                    url
                    state
                    startedAt
                    completedAt
                    canceledAt
                    autoArchivedAt
                    startDate
                    targetDate
                    issueCountHistory
                    scopeHistory
                    scope
                    id
                    teams {{
                        nodes {{
                            id
                        }}
                    }}
                }}
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
            }}
        }}
    """
    return fetch_linear_graphql_api(api_key=api_key, query=query, entity="projects")


@dlt.resource(
    name="projects",
    write_disposition="merge",
    columns={  # don't flatten lists into child tables
        "teams": {"data_type": "complex"},
        "scopeHistory": {"data_type": "complex"},
        "issueCountHistory": {"data_type": "complex"},
    },
    primary_key="id",
)
def resource(
    api_key: str, updated_t: dlt.sources.incremental[str]
) -> t.Iterator[t.List[t.Dict[str, t.Any]]]:
    date_lbound = isoparse(str(updated_t.last_value)).date()
    projects_response = _fetch_linear_projects(api_key=api_key, date_lbound=date_lbound)

    for project in projects_response["nodes"]:
        yield project

    while projects_response["pageInfo"]["hasNextPage"]:
        projects_response = _fetch_linear_projects(
            api_key=api_key,
            date_lbound=date_lbound,
            next_cursor=projects_response["pageInfo"]["endCursor"],
        )
        for project in projects_response["nodes"]:
            yield project
