WITH
-- 国家大剧院的坐标（WGS84）
theater_point AS (
    SELECT ST_SetSRID(ST_MakePoint(116.397, 39.908), 4326) AS geom
),
-- 计算每条河流到国家大剧院的距离（单位：米）
river_distances AS (
    SELECT
        r.name,
        ST_Distance(theater_point.geom, r.geom) AS distance_meters
    FROM gis_rivers r
    CROSS JOIN theater_point
    WHERE ST_GeometryType(r.geom) IN ('ST_LineString', 'ST_MultiLineString')
)
-- 取距离最远的一条河流
SELECT name
FROM river_distances
ORDER BY distance_meters DESC
LIMIT 1;
