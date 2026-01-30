SELECT 
    name AS river_name,
    ST_Distance(
        ST_SetSRID(ST_MakePoint(116.397, 39.908), 4326),
        geom
    ) AS distance_meters
FROM gis_water
ORDER BY distance_meters ASC
LIMIT 3;
