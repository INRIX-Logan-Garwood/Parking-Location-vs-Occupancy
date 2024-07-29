 SELECT trip.trip_id,
    trip.device_id,
    trip.end_qk,
    trip.provider_id,
    traj.traj_idx,
    traj.traj_raw_distance_m,
    traj.traj_raw_duration_millis,
    sol.point_idx,
    sol.utc_ts,
    sol.raw_lat,
    sol.raw_lon
FROM trajectories.trajectories trip
    CROSS JOIN UNNEST(trajectories) as t(traj)
    CROSS JOIN UNNEST(traj.solution_segments) as t(seg)
    CROSS JOIN UNNEST(seg.solution_snaps) as t(sol)
WHERE trip.map = 'osm'
    AND trip.mapversion = '20221201'
    AND trip.region = 'na'
    AND trip.year in ('2023')
    AND trip.month in ('01')
    AND trip.day in ('01', '02', '03', '04')
    AND ((trip.end_qk LIKE '0230132021020101%') OR
         (trip.end_qk LIKE '0230132021020110%') OR
         (trip.end_qk LIKE '0230132021020100%') OR
         (trip.end_qk LIKE '0230132021002323%'))