"""Reusable Bronze-layer helpers."""


def resolve_monthly_path(landing_base, dataset, target_yyyymm):
    return "{0}/{1}/{2}/*.csv".format(landing_base, dataset, target_yyyymm)


def resolve_dimension_path(landing_base, table_name, target_yyyymm):
    return "{0}/dimensions/{1}/{2}.csv".format(landing_base, target_yyyymm, table_name)


def build_replace_where(partition_column, partition_values):
    ordered_values = sorted(set(partition_values))
    if not ordered_values:
        raise ValueError("partition_values must not be empty")
    quoted_values = ",".join(repr(value) for value in ordered_values)
    return "{0} IN ({1})".format(partition_column, quoted_values)

