import datetime as dt
import typing as t

import dlt
from dateutil.parser import isoparse

from .utils import fetch_linear_graphql_api


def _fetch_linear_issues(
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
            issues(
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
                    title
                    description
                    creator {{
                        name
                        email
                    }}
                    assignee {{
                        name
                        email
                    }}
                    identifier
                    createdAt
                    updatedAt
                    startedTriageAt
                    triagedAt
                    url
                    dueDate
                    startedAt
                    completedAt
                    canceledAt
                    autoArchivedAt
                    autoClosedAt
                    dueDate
                    team {{
                        id
                    }}
                    project {{
                        id
                    }}
                    parent {{
                        id
                    }}
                    state {{
                        type
                    }}
                    id
                }}
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
            }}
        }}
    """
    return fetch_linear_graphql_api(api_key=api_key, query=query, entity="issues")


@dlt.resource(name="issues", write_disposition="merge", primary_key="id")
def resource(
    api_key: str, updated_at: dlt.sources.incremental[str]
) -> t.Iterator[t.List[t.Dict[str, t.Any]]]:
    date_lbound = isoparse(str(updated_at.last_value)).date()
    issues_response = _fetch_linear_issues(api_key=api_key, date_lbound=date_lbound)

    for issue in issues_response["nodes"]:
        yield issue

    while issues_response["pageInfo"]["hasNextPage"]:
        issues_response = _fetch_linear_issues(
            api_key=api_key,
            date_lbound=date_lbound,
            next_cursor=issues_response["pageInfo"]["endCursor"],
        )
        for issue in issues_response["nodes"]:
            yield issue
