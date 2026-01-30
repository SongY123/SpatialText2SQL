SELECT ST_AsText(ST_Centroid(ST_GeomFromText('POINT(116.397 39.908)', 4326))) AS nearest_river_center_point_wkt;
