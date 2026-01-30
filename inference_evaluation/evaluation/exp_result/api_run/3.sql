SELECT 
    r.id,
    r.name,
    ST_Distance(
        ST_Transform(r.geom, 3857),
        ST_Transform(ST_SetSRID(ST_MakePoint(116.397, 39.908), 4326), 3857)
    ) AS distance_meters
FROM 
    gis_rivers r
WHERE 
    ST_Distance(
        ST_Transform(r.geom, 3857),
        ST_Transform(ST_SetSRID(ST_MakePoint(116.397, 39.908), 4326), 3857)
    ) <= 1000
ORDER BY 
    distance_meters ASC;
