import itertools
import logging

# I'm going to use pandas to mock data structures
# These pandas statements could be transcribed to
# PySpark, SQLAlchemy, or other datasource connectors fairly easily
# But for a small, portable example, pandas is quickest
import pandas as pd

logger = logging.getLogger(__name__)


def compute_metric(data, dimensions, metric_function):
    """a generic metric applicator to a dimension group"""

    # grouping and performing an arbitrary aggregation
    # is an easy and well-known problem, whose solution is implemented in one-line:
    computed = data.groupby(dimensions).apply(metric_function).to_frame()
    computed.columns = ["metric_value"]

    # however, keeping track of *what* the metric does , which fields it aggregates, etc,
    # and easily querying those results, is harder,
    # when the number of dimensions is variable/unknown

    # I'll propose a solution here:
    # Let's have two columns:
    # 1) the metric "label". This would be a unique string that represents what the metric means.
    # e.g. if grouping by age & sex, the metric label might be "CD/CEB for Age/Sex Bucket"
    # and 2) a "parameters" column that relates the specific values for each group . (e.g. Age=35,Sex=Male)
    # We'll make this a JSON-serialized column, because most databases and warehouses
    # support & can easily query a JSON column

    # These two columns will serve as a kind of
    # composite primary key of a generic "metrics" table

    # 1) lets make a meaningful label
    # i.e. describe the metric as "A metric computed over a slice of these dimensions"
    metric_label = ", ".join(dimensions)
    # make the label have the metric name in it as well
    # we'll hook into this nice metric_function attribute for that
    if hasattr(metric_function, "label"):
        metric_label = f"{metric_function.label}: {metric_label}"
        # e.g. "CD/CEB: Age in 5-year groups, Type of place of residence, Has radio"

    # make the parameters column
    def to_params(group):
        """
        transform the group index into "parameters" json struct
        """
        # NOTE: there is probably a better way to do this, but this works:
        # Get the first value from the dimension column from the group
        # (since it is grouped by the dim, the values in this column are guarenteed to be identical)
        index_keys = {dimension: group[dimension].values[0] for dimension in dimensions}

        # NOTE: could implement a more serializer/type-caster here
        # e.g.`json.dumps` or maybe marshmallow validation
        # depending on where this data is going.
        return index_keys

    labeled = data.groupby(dimensions).apply(to_params).to_frame()
    labeled.columns = ["parameters"]

    # add labels to parameters
    # NOTE: this could be done more efficiently by computing labels and parameters BOTH
    # within the same groupby
    # rather than computing separately and joining back together
    result = computed.merge(labeled, left_index=True, right_index=True)
    result["metric_label"] = metric_label
    logger.info(f"Computed {result.shape[0]:0,} results for {metric_label}")
    return result


def dimension_subsets(dimensions):
    """ implement a dimension-combination generator"""
    # "Every possible combination" means all combinations
    # up to the total number of dimensions (in this case, 3).
    # So, that would mean 1, 2, and 3-dimensional cuts of the metric.

    # loop thru number of dimensions
    for i in range(len(dimensions)):
        # yield combinations of length i
        for c in itertools.combinations(dimensions, i + 1):
            yield c
