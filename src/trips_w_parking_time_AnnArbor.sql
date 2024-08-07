SELECT 
    trip_id,
    provider_id,
    start_lat,
    start_lon,
    end_lat,
    end_lon,
    trip_mean_speed,
    trip_max_speed,
    trip_distance_m,
    MIN(p.capture_time) AS entry_time,
    MAX(p.capture_time) AS stop_time
FROM inrixdatascience.tripdata_na_restricted t
CROSS JOIN UNNEST(points) AS t(p)
WHERE year = '2023'
AND month = '01'
AND day in ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23')
AND qk in ('03022')
AND is_moving = 1
AND (
    ST_Intersects(ST_Point(p.lon, p.lat), ST_Polygon('POLYGON ((-83.74760999999999 42.27801, -83.74800999999999 42.27803, -83.74797 42.27892, -83.74757 42.27891, -83.74758 42.27887, -83.74751999999999 42.27887, -83.74755 42.27806, -83.74759 42.27806, -83.74760999999999 42.27801))'))
    OR ST_Intersects(ST_Point(p.lon, p.lat), ST_Polygon('POLYGON ((-83.73326 42.27392, -83.73331 42.27392, -83.7333 42.27386, -83.73407 42.27386, -83.73408999999999 42.27434, -83.73327 42.27435, -83.73326 42.27392))'))
    OR ST_Intersects(ST_Point(p.lon, p.lat), ST_Polygon('POLYGON ((-83.74916 42.28315, -83.7492 42.28212, -83.74957999999999 42.28213, -83.74952999999999 42.28316, -83.74916 42.28315))'))
    OR ST_Intersects(ST_Point(p.lon, p.lat), ST_Polygon('POLYGON ((-83.74285 42.27965, -83.74285 42.27969, -83.74290000000001 42.27969, -83.74288 42.2802, -83.74284 42.2802, -83.74284 42.28022, -83.7423 42.2802, -83.7423 42.28016, -83.74225 42.28015, -83.74227 42.27986, -83.74244 42.27986, -83.74245000000001 42.27964, -83.74285 42.27965))'))
    OR ST_Intersects(ST_Point(p.lon, p.lat), ST_Polygon('POLYGON ((-83.74786 42.28092, -83.74741 42.28091, -83.74742999999999 42.28057, -83.74787999999999 42.28058, -83.74786 42.28092))'))
    OR ST_Intersects(ST_Point(p.lon, p.lat), ST_Polygon('POLYGON ((-83.74311 42.27852, -83.74311 42.27888, -83.74263999999999 42.27889, -83.74265 42.27883, -83.74144 42.27884, -83.74142000000001 42.27851, -83.74261 42.27848, -83.74261 42.27854, -83.74311 42.27852, -83.74311 42.27852))'))
    OR ST_Intersects(ST_Point(p.lon, p.lat), ST_Polygon('POLYGON ((-83.74598 42.27899, -83.74529 42.27897, -83.74531 42.27865, -83.74598 42.27868, -83.74598 42.27899))'))
    )
GROUP BY 
    trip_id,
    provider_id,
    start_lat,
    start_lon,
    end_lat,
    end_lon,
    trip_mean_speed,
    trip_max_speed,
    trip_distance_m