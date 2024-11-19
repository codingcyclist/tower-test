import datetime as dt
import typing as t

import dlt
from dateutil.parser import isoparse

from .utils import fetch_linear_graphql_api


def _fetch_linear_issue_relations(
    api_key: str,
    date_lbound: dt.date,
    page_size: int = 10,  # we must keep query complexity < 10.000
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
                    relations {{
                        nodes {{
                            id
                            type
                            updatedAt
                            relatedIssue {{
                                id
                            }}
                        }}
                    }}
                    id
                    updatedAt
                }}
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
            }}
        }}
    """
    return fetch_linear_graphql_api(api_key=api_key, query=query, entity="issues")


@dlt.resource(
    name="issue_relations", write_disposition="merge", primary_key="relation_id"
)
def resource(
    api_key: str, updated_at: dlt.sources.incremental[str]
) -> t.Iterator[t.List[t.Dict[str, t.Any]]]:
    date_lbound = isoparse(str(updated_at.last_value)).date()
    issue_relations_response = _fetch_linear_issue_relations(
        api_key=api_key, date_lbound=date_lbound
    )

    def flatten_issue_relations(response):
        for issue in response["nodes"]:
            for relation in issue["relations"]["nodes"]:
                yield {
                    "relation_id": relation["id"],
                    "issue_id": issue["id"],
                    "related_issue_id": relation["relatedIssue"]["id"],
                    "relations_type": relation["type"],
                    "updatedAt": max(issue["updatedAt"], relation["updatedAt"]),
                }

    yield from flatten_issue_relations(issue_relations_response)

    while issue_relations_response["pageInfo"]["hasNextPage"]:
        issue_relations_response = _fetch_linear_issue_relations(
            api_key=api_key,
            date_lbound=date_lbound,
            next_cursor=issue_relations_response["pageInfo"]["endCursor"],
        )
        yield from flatten_issue_relations(issue_relations_response)
