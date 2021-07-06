import json
import logging
import sys

import pandas as pd
import pytest

import solution

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


# our example metric calculation:
def custom_metric(group):
    """ implement the metric defined as CD/CEB, within a particular group """
    children_died = (
        group["Sons who have died"] + group["Daughters who have died"]
    ).sum()
    children_ever_born = group["Total children ever born"].sum()
    return children_died / children_ever_born


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
        # dimensions_to_combine[:],
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
            -- a FK to a `metric_labels` table
            -- that included more info about the metric type,
            -- such as an array column of the dimensions that it groups by
            parameters jsonb,
            metric_value numeric,
        )
        """

        # or this might be a good time to put it in Mongo if you use that.

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


@pytest.fixture
def assert_algo_validity():
    """
    continuously ensure our algorithm
    correctly filters out below-threshold combinations
    """
    # let's take note of the dimension subsets we're filtering out
    # we are going to use them to make some assertions
    # about the correctness of the algorithm

    # this dict will hold *all* filters made
    to_ignore = {}

    # this func will check all filters for all dimension subsets
    # and make sure its not in the raw data
    def ensure_no_blacklisted_data(dimensions, ignored_combinations, raw_data):
        """
        check all combos of ignored dimension subsets
        and make sure they are not in the raw data
        """
        # this is a test-only function
        # let's take a copy of the data so as not to mutate the dataframe
        raw_data = raw_data.copy()

        to_ignore[tuple(dimensions)] = ignored_combinations
        for dims, combinations in to_ignore.items():
            for combo in combinations.index.tolist():
                if not hasattr(combo, "__iter__"):
                    # special case for 1-element sets
                    combo = (combo,)
                # there is probably a better way to do this
                # but for now, just iteratively filter down raw data
                for dim, val in zip(dims, combo):
                    raw_data = raw_data[raw_data[dim] == val]
                # and after all that filtering it should be empty
                assert raw_data.empty

    yield ensure_no_blacklisted_data


@pytest.mark.parametrize(
    # run the test on a set of dimensions to combine
    "dimensions",
    [
        dimensions_to_combine[0:2],
        dimensions_to_combine[3:6],
        # you uncomment the full `dimensions_to_combine` and run them here
        # but since it is slow (many permutations) I have it commented out:
        # dimensions_to_combine[:],
    ],
)
@pytest.mark.parametrize("threshold", [1, 5, 10, 50])  # example row threshold
def test_problem_2(data, threshold, dimensions, assert_algo_validity):
    """ PROBLEM 2 IMPLEMENTATION -----------------"""
    # Develop an algorithm that only computes the ratio metric
    # for a narrower subset of combinations
    # that each meet a minimum threshold of rows.

    results = []

    # loop thru combinations
    # from 1-element combinations to len(dimensions)
    for dimension_tuple in solution.dimension_subsets(dimensions):
        # calculate the count of values for each dimension group
        group = list(dimension_tuple)
        counts = data.groupby(group).agg({data.columns[0]: "count"})
        counts.columns = ["n_observations"]
        # note which one's we're filtering out for testing algo correctness
        filtered_out = counts[counts.n_observations < threshold]
        # filter out dimension groups below the threshold
        counts = counts[counts.n_observations >= threshold]
        # this inner join eliminates the dimension groups below the threshold from the data
        # for this and all future iterations of the loop
        data = data.merge(counts, left_on=group, right_index=True)
        # test that our filter worked
        assert_algo_validity(group, filtered_out, data)
        # compute the result
        result = solution.compute_metric(data, dimensions, custom_metric)
        # send it to outer space.
        results.append(result)
