import json
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
    # run the test on a set of dimensions to combine
    "dimensions",
    [
        dimensions_to_combine[0:2],
        dimensions_to_combine[3:6],
        # you uncomment the full `dimensions_to_combine` and run them here
        # but since it is slow (many permutations) I have it commented out:
        # dimensions_to_combine[:]
    ],
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
    # and i'll prove it here:
    df = pd.concat(results)
    assert len(df.columns) == 3
    assert all(set(res.columns) == set(df.columns) for res in results)

    # also i can prove that my composite primary key is unique:
    n_rows = df.shape[0]

    # but to do so i'll have to convert parameters to a string to make it hashable:
    df["parameters"] = df.parameters.apply(lambda x: json.dumps(x, default=str))

    # then i can compute unique rows
    n_unique_rows = df[["metric_label", "parameters"]].drop_duplicates().shape[0]

    # and prove the PK is unique
    assert n_rows == n_unique_rows


#
# def test_problem_2(data):
#     """ PROBLEM 2 IMPLEMENTATION -----------------"""
#     # Develop an algorithm that only computes the ratio metric
#     # for a narrower subset of combinations
#     # that each meet a minimum threshold of rows.
#
#     # ^^ What does this mean "each meet a minimum threshold of rows"
#
#     def groom(data, dimensions, threshold):
#         """ groom data such that combos with a count below a certain threshold arent used """
#         pass
