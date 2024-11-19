import typing as t

import dlt

from .utils import fetch_linear_graphql_api


def _fetch_linear_teams(
    api_key: str,
    page_size: int = 100,
    next_cursor: t.Optional[str] = None,
) -> t.Dict[str, t.Any]:
    query = f"""
        {{
            teams(
                first: {page_size}
                filter: {{
                    or: {{
                        createdAt: {{
                            gt: "2022-07-01T00:00:00"
                        }},
                        updatedAt: {{
                            gt: "2022-07-01T00:00:00"
                        }}
                    }}
                }}
                after: {'null' if next_cursor is None else '"' + next_cursor + '"'}
                includeArchived: true
            ) {{
                nodes {{
                    name
                    description
                    createdAt
                    updatedAt
                    archivedAt
                    private
                    id
                }}
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
            }}
        }}
    """
    return fetch_linear_graphql_api(api_key=api_key, query=query, entity="teams")


@dlt.resource(name="teams", write_disposition="replace", primary_key="id")
def resource(api_key: str) -> t.Iterator[t.List[t.Dict[str, t.Any]]]:
    teams_response = _fetch_linear_teams(api_key=api_key)

    for team in teams_response["nodes"]:
        yield team

    while teams_response["pageInfo"]["hasNextPage"]:
        teams_response = _fetch_linear_teams(
            api_key=api_key,
            next_cursor=teams_response["pageInfo"]["endCursor"],
        )
        for team in teams_response["nodes"]:
            yield team
