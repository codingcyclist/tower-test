"""
This is a module that provides a DLT source to retrieve data from multiple entities
within the Linear GraphQL API using a personal API key.

The retrieved data is returned as a tuple of Dlt resources, one for each entity.

The source retrieves data from the following entities:
- Projects
- Issues
- Teams

For each entity, a resource function is defined to retrieve data
and transform it to a common format.
"""

import typing as t

import dlt
from dateutil.parser import parse
from dlt.extract.incremental import Incremental
from dlt.extract.source import DltResource

from lake.src.pipelines.linear.resources.issue_relations import (
    resource as issue_relations_resource,
)
from lake.src.pipelines.linear.resources.issues import resource as issues_resource
from lake.src.pipelines.linear.resources.projects import resource as projects_resource
from lake.src.pipelines.linear.resources.teams import resource as teams_resource


@dlt.source(name="linear")
def linear(
    api_key: str = dlt.secrets.value, start_date=dlt.config.value
) -> t.Sequence[DltResource]:
    """
    A DLT source that retrieves data from Linear's GraphQL API
    using a personalized API key.

    This function retrieves data for several Linear entities,
    including projects, issues, labels, and teams.
    It returns a tuple of Dlt resources, one for each endpoint.

    Args:
        api_key (str, optional): The API key used to authenticate with the Linear API.
            Defaults to dlt.secrets.value.
        start_date (str, optional): If provided, will only load issues created or updated
            after the given date

    Returns:
        tuple: A tuple of Dlt resources, one for each entity in
            the Linear API (issues / projects / teams).
    """

    resource_updated_at_lbound: Incremental[str] = dlt.sources.incremental(
        "updatedAt", initial_value=parse(start_date).isoformat()
    )
    return [
        projects_resource(
            api_key,
            resource_updated_at_lbound,
        ),
        issues_resource(
            api_key,
            resource_updated_at_lbound,
        ),
        teams_resource(
            api_key,
        ),
        issue_relations_resource(
            api_key,
            resource_updated_at_lbound,
        ),
    ]
