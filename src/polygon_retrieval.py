'''
Query parking lot polygons

Need to be on Analytics account to establish the athena connection
'''

import inrix_data_science_utils.api.athena as athena
import inrix_data_science_utils.dates as idt



def get_polygons(brand_id, format='df', echo_query=False):
    '''
    Query parking lot polygons
    '''
    conn = athena.create_athena_connection(
        s3_staging_dir=athena.S3_STAGING_DIR["analytics"],
        key_check=False,
        profile="analytics",
        work_group="primary",
    )

    if not brand_id:
        # if not specified then use 6 flags
        brand_id = 'SG_BRAND_0dd52fbf1cd77fc38e06650435ada07d'

    query = f'''
        select poi.*, prk.polygon_wkt as parking_wkt
        from sg_poi.poi_staging_pg_partitions_202404 poi
        join sg_poi.parking_staging_pg_partitions prk on poi.placekey = prk.related_poi[1]
        where cardinality(related_poi) > 0 and cardinality(brands) > 0
        and brands[1].safegraph_brand_id = '{brand_id}'
    '''

    if echo_query:
        print(query)

    # format can be either df or download or tuple
    if format == 'df':
        results = athena.pandas_sql(conn, query)
    elif format == 'dowload':
        results = athena.execute_sql(conn, query, retrieve=True)
    elif format == 'tuple':
        results = athena.execute_sql(conn, query, retrieve=False)
    
    if echo_query: 
        print("Query executed successfully.")

    return results

def main():
    '''
    Main function
    '''
    data = get_polygons(format='df', echo_query=True)
    print(data)

if __name__ == "__main__":
    main()