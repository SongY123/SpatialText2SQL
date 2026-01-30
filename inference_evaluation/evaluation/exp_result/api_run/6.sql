SELECT 
    CASE 
        WHEN COUNT(*) > 0 THEN '是' 
        ELSE '否' 
    END AS 是否相交
FROM 
    gis_data 
WHERE 
    ST_Intersects(
        ST_Buffer(
            ST_SetSRID(ST_MakePoint(116.397128, 39.908722), 4326), 
            2000, 
            'quad_segs=30'
        ),
        geom
    )
    AND ST_SRID(geom) = 4326;
