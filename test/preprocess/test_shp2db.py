import unittest
import os
import sys

# Add repository root to path so we can import the src package.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.preprocess.shp2db import shp2db

class TestShp2Db(unittest.TestCase):
    def setUp(self):
        # Configuration for tests
        # Use absolute paths to avoid GDAL/Fiona issues with relative paths
        self.shp_dir = os.path.abspath("../dataset/osm/beijing-251222")
        
        # PostGIS connection (Update with your actual credentials if needed for local testing)
        self.pg_db_url = "postgresql://postgres:123456@localhost:5432/postgres"
        self.pg_schema = "osm"
        
        # SpatiaLite connection
        self.sl_db_path = os.path.abspath("../dataset/osm/test_spatial.sqlite")
        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.sl_db_path), exist_ok=True)
        self.sl_db_url = f"sqlite:///{self.sl_db_path}"

        # Clean up previous sqlite file if exists
        if os.path.exists(self.sl_db_path):
            os.remove(self.sl_db_path)

    def test_import_directory_to_postgis(self):
        print("\nTesting import directory to PostGIS...")
        try:
            shp2db(
                input_path=self.shp_dir,
                db_url=self.pg_db_url,
                schema=self.pg_schema,
                if_exists='replace'
            )
            print("PostGIS import successful.")
        except Exception as e:
            self.fail(f"PostGIS import failed: {e}")

    def test_import_directory_to_spatialite(self):
        print("\nTesting import directory to SpatiaLite...")
        try:
            shp2db(
                input_path=self.shp_dir,
                db_url=self.sl_db_url,
                if_exists='replace'
            )
            print("SpatiaLite import successful.")
            self.assertTrue(os.path.exists(self.sl_db_path), "SpatiaLite DB file should exist")
        except Exception as e:
            self.fail(f"SpatiaLite import failed: {e}")

if __name__ == '__main__':
    unittest.main()
