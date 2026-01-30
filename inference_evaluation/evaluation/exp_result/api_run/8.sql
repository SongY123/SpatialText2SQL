要计算国家大剧院附近 500 米内每条河的**最小距离（近似）**，我们需要以下步骤：

---

### ✅ 假设与前提

1. **国家大剧院的位置**：
   - 国家大剧院位于中国北京，坐标为：
     - 经度：116.397°E
     - 纬度：39.908°N
   - 使用 WGS84 坐标系（EPSG:4326）

2. **河流数据表**：
   - 表名：`gis_rivers`
   - 字段：
     - `id`: 河流编号
     - `name`: 河流名称
     - `geom`: 几何字段（LINESTRING 或 MULTILINESTRING）

3. **空间参考系统（SRID）**：
   - 如果河流数据使用的是地理坐标（如 WGS84），则需要转换为投影坐标（如 Web Mercator 或当地投影）以进行准确的距离计算。
   - 如果河流数据使用的是投影坐标（如 EPSG:3857），则可直接使用。

---

### 🧠 思路

1. 构造一个以国家大剧院为中心、半径为 500 米的缓冲区（Buffer）。
2. 使用空间查询找出该缓冲区内的所有河流。
3. 计算缓冲区与每条河流之间的**最小距离**（使用 `ST_Distance`）。
4. 返回每条河流的名称和其最小距离。

---

### 🧾 SQL 查询（PostGIS）

```sql
WITH center_point AS (
    SELECT ST_SetSRID(ST_MakePoint(116.397, 39.908), 4326) AS geom
),
buffer_500m AS (
    SELECT ST_Buffer(geom, 500) AS buffer_geom
    FROM center_point
),
rivers_in_buffer AS (
    SELECT r.id, r.name, r.geom
    FROM gis_rivers r
    JOIN buffer_500m b
    ON ST_Intersects(r.geom, b.buffer_geom)
)
SELECT 
    r.name AS river_name,
    ST_Distance(
        ST_SetSRID(ST_MakePoint(116.397, 39.;
