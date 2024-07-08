'''
File from DataScience-TripsAsParkingDemandEstimates
'''

import re
import pandas as pd
import boto3
import inrix_data_science_utils.api.athena as athena

MOVEMENT = 'moving'  # default is 'moving', but can be 'stopped'


def get_providers_and_quadkeys(s3_tapp_data_dir, s3_tapp_region, start_date):
    """
    Looks at the TAPP (trips) data in S3 for the given date and returns the
    provider and quadkey partitions.
    """
    start_year = start_date.year
    start_month = start_date.month
    start_day = start_date.day

    aws_session = boto3.session.Session(profile_name="analytics")
    s3 = aws_session.resource("s3")
    tapp_bucket = s3.Bucket("inrixprod-tapp")
    prefix = f"{s3_tapp_data_dir}/region={s3_tapp_region}/movement_type={MOVEMENT}/year={start_year}/month={start_month:02}/day={start_day:02}/"

    s3_objects = [
        object_summary.key
        for object_summary in tapp_bucket.objects.filter(Prefix=prefix)
        if "_$folder$" not in object_summary.key
    ]

    s3_objects_splits = [
        re.sub("/[^/]*\.gz\.parquet", "", o.replace(prefix, "")).split("/")
        for o in s3_objects
        if o != "_SUCCESS"
    ]
    
    providers, quadkeys = zip(*s3_objects_splits)
    unique_providers = list(set([p.replace("provider=", "") for p in providers]))
    unique_quadkeys = list(set([q.replace("qk=", "") for q in quadkeys]))
    return unique_providers, unique_quadkeys


def get_agg_trips(
    agg_file_path,
    echo_query,
    table_name,
    start_date,
    end_date,
    qk_filter_list,
    s3_tapp_data_dir,
    s3_tapp_region,
    origin_qk
):
    """
    Retrieve the data using Athena.
    """
    
    # Get providers for the data, but omit provider 457 (Life360) because it
    # will contain non vehicle trips. We can probably do better than this.
    providers, quadkeys = get_providers_and_quadkeys(
        s3_tapp_data_dir=s3_tapp_data_dir,
        s3_tapp_region=s3_tapp_region,
        start_date=start_date,
    )
    
    providers = [p for p in providers if p != "457"]


    qk_level_5 = list(set([qk[:5] for qk in qk_filter_list]))

    other_partition_fields={"provider": providers, 
                            "qk": qk_level_5}
    
    conn = athena.create_athena_connection(
        s3_staging_dir=athena.S3_STAGING_DIR["analytics"],
        key_check=False,
        profile="analytics",
        work_group="data-science",
    )

    # Partition the Athena table.
    timestamps = pd.date_range(start_date, end_date)

    # Add in day of month extractor.
    field_extractors = {
        "year": athena.year_extractor,
        "month": athena.month_extractor,
        "day": athena.day_extractor,
    }

    # For each timestamp, create relevant partition.
    new_partitions = athena.extract_partition_data(
        timestamps=timestamps,
        partition_names=("year", "month", "day", "provider", "qk"),
        field_extractors=field_extractors,
        other_partition_fields=other_partition_fields,
    )
    partition_add_query = athena.create_update_partitions_query(
        f"inrixdatascience.{table_name}", new_partitions
    )
    athena.execute_sql(conn, partition_add_query, expect_data=False)

    def make_qk_clause(qk_list, origin_qk):
        """
        Takes in a list of quadkeys and constructs an SQL statement to restrict
        the query to that region.
        origin_qk = Boolean: controls which part of the trip to use for the selection.
        origin_qk=False (default) will use the destination lat/lon to define the region
        whereas origin_qk=True will use the lat/lon of the trip's beginning.
        """
        if qk_list:
            qk_lengths = set([len(q) for q in qk_list])
            if len(qk_lengths) == 1:
                qk_level = qk_lengths.pop()
                if origin_qk:
                    return f"""AND REGEXP_LIKE(
                        BING_TILE_QUADKEY(BING_TILE_AT(start_lat, start_lon, {qk_level})),
                        '({'|'.join(qk_list)})'
                    )
                    """
                else:
                    return f"""AND REGEXP_LIKE(
                        BING_TILE_QUADKEY(BING_TILE_AT(end_lat, end_lon, {qk_level})),
                        '({'|'.join(qk_list)})'
                    )
                    """
            else:
                raise ValueError("Quadkeys are not all the same length.")
        else:
            return ""

    def make_partition_clause(prefix, qk_partition_list):
        if len(qk_partition_list) == 1:
            return f"{prefix} = '{qk_partition_list[0]}'"
        else:
            return f"{prefix} IN {tuple(qk_partition_list)}"

    # query = f"""
    #         WITH qk_counts AS(
    #         SELECT start_time, provider, start_lat, start_lon, end_lat, end_lon, 
    #                 BING_TILE_QUADKEY(BING_TILE_AT(end_lat, end_lon, 17)) AS dest_qk17,
    #                 BING_TILE_QUADKEY(BING_TILE_AT(start_lat, start_lon, 17)) AS orig_qk17,
    #                 year, month, day, SUBSTR(start_time,12, 2) AS hour, trip_id, is_moving

    #         FROM "inrixdatascience"."{table_name}"
    #         WHERE
    #             {make_partition_clause("qk", other_partition_fields["qk"])}
    #             AND year IN ('{start_date.year}', '{end_date.year}')
    #             AND month IN ('{start_date.month:02}', '{end_date.month:02}')
    #             AND CAST(day AS INT) BETWEEN {start_date.day} AND {end_date.day}
    #             AND {make_partition_clause("provider", other_partition_fields["provider"])}
    #             {make_qk_clause(qk_filter_list, origin_qk)}
    #             )
    # SELECT year, month, day, hour, orig_qk17, dest_qk17, start_lat, start_lon, 
    #         end_lat, end_lon, COUNT(*) AS count, trip_id, is_moving
    # FROM qk_counts
    # GROUP BY year, month, day, hour, orig_qk17, dest_qk17, start_lat, start_lon, 
    #         end_lat, end_lon, trip_id, is_moving
    # """

    # add the minute and hour
    # also fix the date range logic 
    # but it still doesn't work like regular dates
    query = f"""
            WITH qk_counts AS(
            SELECT start_time, provider, start_lat, start_lon, end_lat, end_lon, 
                    BING_TILE_QUADKEY(BING_TILE_AT(end_lat, end_lon, 17)) AS dest_qk17,
                    BING_TILE_QUADKEY(BING_TILE_AT(start_lat, start_lon, 17)) AS orig_qk17,
                    year, month, day, SUBSTR(start_time,12, 2) AS hour, SUBSTR(start_time, 15, 2) AS minute,
                    SUBSTR(start_time, 18, 2) AS second, trip_id, is_moving

            FROM "inrixdatascience"."{table_name}"
            WHERE
                {make_partition_clause("qk", other_partition_fields["qk"])}
                AND CAST(year as INT) BETWEEN {start_date.year} AND {end_date.year}
                AND CAST(month as INT) BETWEEN {start_date.month} AND {end_date.month}
                AND CAST(day AS INT) BETWEEN {start_date.day} AND {end_date.day}
                AND {make_partition_clause("provider", other_partition_fields["provider"])}
                {make_qk_clause(qk_filter_list, origin_qk)}
                )
    SELECT start_time, year, month, day, hour, minute, second, orig_qk17, dest_qk17, start_lat, start_lon, 
            end_lat, end_lon, COUNT(*) AS count, trip_id, is_moving
    FROM qk_counts
    GROUP BY start_time, year, month, day, hour, minute, second, orig_qk17, dest_qk17, start_lat, start_lon, 
            end_lat, end_lon, trip_id, is_moving
    """
  
    if echo_query:
        print(query)
    
    trips_out = athena.pandas_sql(conn, query)
    trips_out.to_csv(agg_file_path, index=False)
    return trips_out


def get_agg_trips_by_market(
    market_quadkeys,
    market_name,
    table_name,
    start_date,
    end_date,
    s3_tapp_data_dir,
    s3_tapp_region,
    out_dir,
    echo_query=True,
    origin_qk=False
):
    """
    Retrieve the aggregated trip data. This is a helper function to build the
    output filepath.
    """
    out_filename = f"{market_name}_trips_{start_date}_{end_date}.csv"
    out_file_path = out_dir.joinpath(out_filename)

    # check if the directory exists
    # if not out_dir.exists():
    #     out_dir.mkdir(parents=True)

    return get_agg_trips(
        out_file_path,
        echo_query,
        table_name=table_name,
        start_date=start_date,
        end_date=end_date,
        qk_filter_list=market_quadkeys,
        s3_tapp_data_dir=s3_tapp_data_dir,
        s3_tapp_region=s3_tapp_region,
        origin_qk=origin_qk
    )