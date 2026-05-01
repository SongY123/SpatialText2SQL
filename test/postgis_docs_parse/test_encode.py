import os
import unittest

try:
    import chardet
    import fiona  # noqa: F401
    import geopandas as gpd
except ModuleNotFoundError:
    chardet = None
    gpd = None


class EncodeSmokeTest(unittest.TestCase):
    def test_shapefile_encoding_smoke(self):
        if chardet is None or gpd is None:
            self.skipTest("Optional dependencies not installed: chardet/fiona/geopandas")

        _root = os.path.dirname(os.path.abspath(__file__))
        shp_path = os.path.join(_root, "shp_dir", "gis_osm_roads_free_1.shp")
        if not os.path.exists(shp_path):
            self.skipTest(f"Shapefile not found: {shp_path}")

        dbf_path = shp_path.replace(".shp", ".dbf")
        if not os.path.exists(dbf_path):
            self.skipTest(f"DBF file not found: {dbf_path}")

        with open(dbf_path, "rb") as f:
            raw_data = f.read(10000)
            result = chardet.detect(raw_data)
            self.assertIn("encoding", result)

        test_encodings = ["GBK", "Latin-1", "CP1252", "UTF-8", "GB2312"]
        ok = False
        for enc in test_encodings:
            try:
                _ = gpd.read_file(shp_path, encoding=enc)
                ok = True
                break
            except Exception:
                continue
        self.assertTrue(ok, "No encoding worked for reading the shapefile")


if __name__ == "__main__":
    unittest.main()
