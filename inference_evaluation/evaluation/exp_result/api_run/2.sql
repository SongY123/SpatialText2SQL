SELECT COUNT(*) AS river_count
FROM rivers
WHERE ST_DWithin(
    ST_Transform(rivers.geom, 4326),  -- 假设河流数据是投影坐标，转换为 WGS84
    ST_SetSRID(ST_MakePoint(116.397, 39.908), 4326),  -- 国家大剧院坐标（WGS84）
    1000  -- 1公里 = 1000米
);
