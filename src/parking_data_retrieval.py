'''
Extract the parking occupancy data from postgres database

Need to have Parkme access
'''

from inrix_data_science_utils.api.postgres import PostgresConnector

def construct_query(pk_lot, datetime_start, datetime_end, destination_name, print_query=False):
    def make_pk_lot_clause(pk_lot):
        if pk_lot:
            return f"""AND lot_occupancy.pk_lot = {pk_lot}"""
        else:
            return ""

    # query = f"""
    #     SELECT  lot_occupancy.pk_lot pk_lot,
    #             MIN(dt_start_date) dt_start_date, -- output in UTC
    #             -- MIN(dt_start_date AT TIME ZONE str_timezone) dt_start_date, -- output in local timezone
    #             AVG(i_avail) i_avail,
    #             AVG(f_pct_occ) f_pct_occ,
    #             AVG(i_cap) i_capacity
    #     FROM lot_occupancy
    #     JOIN lot on lot_occupancy.pk_lot = lot.pk_lot
    #     JOIN destination on lot.pk_city = destination.pk_destination
    #     WHERE dt_start_date AT TIME ZONE str_timezone >=  '{str(datetime_start)}'
    #         AND dt_start_date AT TIME ZONE str_timezone <  '{str(datetime_end)}'
    #         AND f_pct_occ IS NOT NULL
    #         AND destination.pk_country = 'b363bb38-ca10-11e1-9278-12313d1b6657' -- USA
    #         AND destination.str_name = '{destination_name}'
    #         {make_pk_lot_clause(pk_lot)}
            
    #     GROUP BY lot_occupancy.pk_lot

    # don't group by pk_lot in order to get regular measurements
    query = f"""
        SELECT  lot_occupancy.*
        FROM lot_occupancy
        JOIN lot on lot_occupancy.pk_lot = lot.pk_lot
        JOIN destination on lot.pk_city = destination.pk_destination
        WHERE dt_start_date AT TIME ZONE str_timezone >=  '{str(datetime_start)}'
            AND dt_start_date AT TIME ZONE str_timezone <  '{str(datetime_end)}'
            AND f_pct_occ IS NOT NULL
            AND destination.pk_country = 'b363bb38-ca10-11e1-9278-12313d1b6657' -- USA
            AND destination.str_name = '{destination_name}'
            {make_pk_lot_clause(pk_lot)}         
        """

    if print_query:
        print(query)

    return query

def get_parking_data(pk_lot, datetime_start, datetime_end, destination_name, echo_query=False):
    '''
    Helper function to maintain consistent naming in data_extraction notebook
    '''
def get_parking_data(pk_lot, datetime_start, datetime_end, destination_name, echo_query=False):
    '''
    Helper function to maintain consistent naming in data_extraction notebook
    '''
    conn = PostgresConnector(
        # host="prod2.cwipgjsh740x.us-west-2.rds.amazonaws.com",
        # host="10.104.16.46",
        # host="10.104.16.215",
        host="devdb-zzz.parkme.com",
        database="pim",
        user="peterparker2",
        password="ppd31MkKZCk6@s^AItlCxQfnf",
        persistent_connection=False,
    )
    query = construct_query(pk_lot, datetime_start, datetime_end,
                            destination_name, print_query=echo_query)
    data = conn.execute_query(query, as_df=True)
    return data

def main():
    pk_lot = None  # Don't need to specify
    destination_name = 'Ann Arbor'
    datetime_start = '2023-01-01'
    datetime_end = '2023-01-02'
    data = get_parking_data(pk_lot, datetime_start, datetime_end, destination_name, echo_query=True)
    print(data.shape)
    print(data)

if __name__ == '__main__':
    main()