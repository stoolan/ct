import logging
import sys

import pandas as pd
import pytest

import solution

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def custom_metric(group):
    """ implement the metric defined as CE/CEB, within a particular group """
    return (group["Sons who have died"] + group["Daughters who have died"]).sum() / (
        group["Total children ever born"].sum()
    )


# lets decorate this function with a nice label we'll hook into
custom_metric.label = "CD/CEB"


@pytest.fixture
def data():
    women = pd.read_csv("women.csv")
    households = pd.read_csv("household.csv")
    # NOTE: "A woman's household can be found using the
    # `Cluster number` and `Household number` columns."
    joined = women.merge(households, on=["Cluster number", "Household number"])
    return joined


dimensions_to_combine = [
    "Age in 5-year groups",
    "Type of place of residence",
    "Number of household members",
    "Source of drinking water",
    "Time to get to water source (minutes)",
    "Type of toilet facility",
    "Has electricity",
    "Has radio",
    "Age of head of household",
]


@pytest.mark.parametrize(
    # run the test on a subset and full set of dimensions to combine
    "dimensions",
    [dimensions_to_combine[0:2], dimensions_to_combine],
)
@pytest.mark.parametrize(
    # run the test on an arbitrary metric function
    "metric_function",
    [custom_metric],
)
def test_problem_1(data, metric_function, dimensions):
    """ PROBLEM 1 IMPLEMENTATION -----------------"""
    # Compute the ratio metric for
    # **every possible combination** of values in the dimensions

    results = []

    # loop thru combinations
    for dimension_tuple in solution.dimension_subsets(dimensions):
        # calc metrics
        result = solution.compute_metric(data, list(dimension_tuple), metric_function)

        # it's calculated right there, now what?
        # it really depends on the use case
        # you might dump it to a table like:
        # result.to_sql('metrics', conn, )

        # If that were the case I would put these in a table like
        """
        CREATE TABLE metrics (
            metric_label string,
            -- in reality, I would make this "metric_label" ^^
            -- a FK to a `metric_types` table
            -- that included more info about the metric type,
            -- such as an array column of the dimensions that it groups by
            parameters jsonb,
            metric_value numeric,
        )
        """

        # but for now i'll append them to a list
        results.append(result)

    # what's nice about this implementation is that I can concatenate all my results
    # because they share the same data format
    df = pd.concat(results)
    return df
