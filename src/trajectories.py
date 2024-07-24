"""API for retrieving trajectories data from AWS Athena.

This is a copy and pasted version in order for me to customize the query to not
necessitate osm segments.
"""
import datetime
import itertools
import typing as tp

import pandas as pd

from inrix_data_science_utils.api import athena
from inrix_data_science_utils.dates import to_date


class TrajectoryAPI(object):
    """API for retrieving trajectory data from AWS Athena.

    This class handles the creation of partitions in the athena table, as well as data
    retrieval. By default it uses the trajectories_restriceted table in athena however
    other sources can be use if they follow the same format and partitions structure.

    Attributes
    ----------
    region_name: str (default us-west-2)
        Region of AWS in which to run queries.
    profile: str (default analytics)
        AWS configuration profile to use. If none is specified then 'analytics' profile will
        be used.
    table_name: str (default trajectories.trajectories_restricted)
        The athena table to use as source for trajectories.
    s3_staging_dir: str (default s3://aws-athena-query-results-861914951438-us-west-2/data-science/)
        Location in S3 where query results will be saved.
    """

    # list of column names in the device stage v2 schema
    VALID_COLUMNS = {
        "trip": [
            "trip_id",
            "device_id",
            "provider_id",
            "points",
            "trip_raw_distance_m",
            "trip_raw_duration_millis",
            "start_utc_ts",
            "end_utc_ts",
            "timezone",
            "error_codes",
            "start_qk",
            "end_qk",
        ],
        "traj": [
            "traj_idx",
            "raw_points",
            "traj_raw_distance_m",
            "traj_raw_duration_millis",
        ],
        "seg": [
            "segment_id",
            "segment_idx",
            "edge_id",
            "base_node",
            "adj_node",
            "length_m",
            "start_offset_m",
            "end_offset_m",
            "start_utc_ts",
            "end_utc_ts",
            "speed_kph",
            "snap_count",
            "on_road_snap_count",
            "error_codes",
            "raw_speed_kph",
            "target_seg_start_offset_m",
            "target_seg_end_offset_m",
            "source_segment_id",
            "service_code",
            "highway_code",
            "solution_snaps",
        ],
    }

    def __init__(
        self,
        region_name: str = "us-west-2",
        profile: str = "analytics",
        table_name: str = "trajectories.trajectories_restricted",
        s3_staging_dir: str = "s3://aws-athena-query-results-861914951438-us-west-2/data-science/",
        work_group: str = "data-science",
    ):
        self.region_name = region_name
        self.profile = profile
        self.table_name = table_name
        self.s3_staging_dir = s3_staging_dir
        self.work_group = work_group

    def create_athena_connection(self):
        """Get a connection to Athena with which to query.

        Returns
        -------
        PyAthena connection instance
        """
        return athena.create_athena_connection(
            s3_staging_dir=self.s3_staging_dir,
            region_name=self.region_name,
            profile=self.profile,
            work_group=self.work_group,
        )

    def create_partitions(
        self,
        mapversion: str,
        region: tp.List[str],
        years: tp.List[str],
        months: tp.List[str],
        days: tp.List[str],
        providers: tp.List[str],
        qks: tp.List[str],
        auto_run: bool = True,
    ):
        """Formats query to add needed partitions to trajectories table, and runs query.
        Lists are passed in for each of the partitions (except mapversion and region) and
        the function adds the cartesian product of all of them as new partitions.

        Parameters
        ----------
        mapversions: str
            The map version that this partition covers. Should have form like '20220601'.
        region: str
            The region to add partitions for, 'na','eu', or 'asia'.
        years: list of str
            The years to add partitions for.
        months: list of str
            The months to add partitions for.
        days: list of str
            The days to add partitions for.
        providers: list of str
            The provider ids to add partitions for. You can match providers to their ids
            using the dmt-dashboard. https://dmt-dashboard.inrix.com/
        qks: list of str
            The level 8 QuadKeys to add partitions for. If you are unsure about which qks
            you need you can use the utils library inrix_data_science_utils.maps.quadkey.
            The function QuadKey.from_geo([lat, lon], 8) returns a level 8 qk for a given
            latitude and longitude.
        auto_run: bool
            If True this function will automatically run the query. It can be set to
            False if you only want to return the query, to make sure you aren't adding
            incorrect partitions.

        Returns
        -------
        string
            Returns the query for adding the partitions.
        """
        query = f"""ALTER TABLE {self.table_name} ADD IF NOT EXISTS \n"""
        for mv, r, y, m, d, p, qk in itertools.product(
            [mapversion], [region], years, months, days, providers, qks
        ):
            query += f"PARTITION (`map` = 'osm', mapversion='{mv}', region='{r}', "
            # query += f"PARTITION (mapversion='{mv}', region='{r}', "
            # query += f"year='{y}', month='{m}', day='{d}', provider='{p}', qk='{qk}')\n"
            query += f"year='{y}', month='{m}', day='{d}', provider='{p}')\n"
        if auto_run:
            cnxn = self.create_athena_connection()
            athena.execute_sql(cnxn, query, expect_data=False)
        return query

    def nested_columns_from_dict(
        self,
        column_dict: tp.Dict[str, tp.List[str]],
    ):
        """Takes a dictionary of the nested columns that are wanted in the query,
        validates that they are actually in the table, then formats a string of the
        SELECT portion of the query.

        Parameters
        ----------
        column_dict: dict
            A dictionary with the desired column names. column_dict should have at least
            one of the keys 'trip', 'traj', 'seg'.

        Returns
        -------
        string
        """
        needed_cols = []
        for nest_lvl in ["trip", "traj", "seg"]:
            needed_cols += [
                f"{nest_lvl}.{c}"
                for c in column_dict.get(nest_lvl, [])
                if c in self.VALID_COLUMNS.get(nest_lvl)
            ]
        assert len(needed_cols) > 0, "No columns found, check format of dict"
        return ",\n".join(needed_cols)

    def trajectories_on_segments(
        self,
        osm_segments: tp.List[str],
        start_time_utc: tp.Union[str, datetime.datetime],
        end_time_utc: tp.Union[str, datetime.datetime],
        map_version: str,
        region: str,
        providers: tp.Optional[tp.List[str]] = None,
        qks: tp.Optional[tp.List[str]] = None,
        columns: tp.Optional[tp.Dict[str, tp.List[str]]] = None,
    ):
        """Returns trajectories for a given set of osm segments.

        The trajectories are run for all of the US on a reccuring basis, and stored in
        s3 in s3://inrixprod-trajectories/

        Data is split into two different folders, 'data' and 'data-restricted'.
        Data-restricted is the one set up with an Athena table so is the default source
        for this class, however other athena tables can be used if they maintain the same
        structure. Just change the variable table_name on init.

        Parameters
        ----------
        osm_segments: list of str
            List of osm segments to return data on. Should be strings in a form similar
            to '-660112843_2'.
        start_time_utc: str or datetime
            Start of the period to retrieve data. Can be either datetime or str.
        end_time_utc: str or datetime
            End of the period to retrieve data. Can be either datetime or str.
        map_version: str
            The osm snapshot to use, if format like '20220601'.
        region: str
            The region to fetch data from, either 'na', 'eu' or 'asia'
        providers: list of str
            Optional parameter. By default it returns all providers it can find, however
            you can pass a list of specific providers to look for.
        qks: list of str
            Optional parameter. If the requested segments are in the quadkeys then it
            doesn't change the output however it does massively speed up the query by
            reducing the amount of data scanned.
        columns: dict of lists of str
            Which columns to return from the query, if left as None it returns all
            columns, as specified in the attribute self.VALID_COLUMNS. Input should be a
            dictionary with keys 'trip', 'traj' and 'seg'. The values of the keys should
            be the corresponding columns at that nested level.
            e.g. columns={'trip':['trip_id'], 'traj':[], seg:['segment_id','speed_kph']}
        """
        # If not passed columns, return all of them
        if columns is None:
            columns = self.VALID_COLUMNS
        # If passed times are string, convert to datetime
        if type(start_time_utc) == str:
            start_time_utc = to_date(start_time_utc)
        if type(end_time_utc) == str:
            end_time_utc = to_date(end_time_utc)
        # If passed providers, add line to where clause
        if providers is None:
            provider_where_clause = ""
        else:
            provider_where_clause = (
                f"""AND trip.provider in ('{"','".join(providers)}')"""
            )
        # If passed qks, add line to where clause
        if qks is None:
            qk_where_clause = ""
        else:
            qk_where_clause = f"""AND trip.end_qk in ('{"','".join(qks)}')"""
        # Get the covering days for partitions
        delta = end_time_utc - start_time_utc
        covering_days = [
            start_time_utc + datetime.timedelta(days=i) for i in range(delta.days + 1)
        ]

        years = {f"{x.year:04}" for x in covering_days}
        months = {f"{x.month:02}" for x in covering_days}
        days = {f"{x.day:02}" for x in covering_days}

        # years = {x.year for x in covering_days}
        # months = {x.month for x in covering_days}
        # days = {x.day for x in covering_days}

        # query = f""" SELECT {self.nested_columns_from_dict(columns)}
        # FROM {self.table_name} trip
        #     CROSS JOIN UNNEST(trajectories) as t(traj)
        #     CROSS JOIN UNNEST(traj.solution_segments) as t(seg)
        # WHERE trip.map = 'osm'
        #     AND trip.mapversion = '{map_version}'
        #     AND trip.region = '{region}'
        #     AND trip.year in ('{"','".join(years)}')
        #     AND trip.month in ('{"','".join(months)}')
        #     AND trip.day in ('{"','".join(days)}')
        #     {provider_where_clause}
        #     {qk_where_clause}
        # """

        query = f""" SELECT {self.nested_columns_from_dict(columns)}
        FROM {self.table_name} trip
            CROSS JOIN UNNEST(trajectories) as t(traj)
            CROSS JOIN UNNEST(traj.solution_segments) as t(seg)
        WHERE trip.map = 'osm'
            AND trip.mapversion = '{map_version}'
            AND trip.region = '{region}'
            AND trip.year in ('{"','".join(years)}')
            AND trip.month in ('{"','".join(months)}')
            AND trip.day in ('{"','".join(days)}')
            {provider_where_clause}
            {qk_where_clause}
        """
        print(query)

        cnxn = self.create_athena_connection()
        data = athena.execute_sql(cnxn, query)
        return pd.DataFrame(data, columns=sum(columns.values(), []))