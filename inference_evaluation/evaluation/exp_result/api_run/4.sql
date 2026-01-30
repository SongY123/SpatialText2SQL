WITH npc_point AS (
    SELECT ST_SetSRID(ST_MakePoint(116.397, 39.908), 4326) AS geom
),
nearest_river AS (
    SELECT
        r.name,
        ST_Distance(npc_point.geom, r.geom) AS distance
    FROM gis_rivers r, npc_point
    ORDER BY ST_Distance(npc_point.geom, r.geom)
    LIMIT 1
)
SELECT name, distance
FROM nearest_river;
