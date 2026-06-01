import unittest

from src.utils.execution_results import compare_result_rows, normalize_execution_value


class ExecutionResultsTests(unittest.TestCase):
    def test_compare_treats_geometry_type_paren_spacing_as_equivalent(self):
        matched, diff = compare_result_rows(
            [("POINT(-75.2640255 40.7564615)",)],
            [("POINT (-75.2640255 40.7564615)",)],
            left_name="predicted",
            right_name="gold",
        )

        self.assertTrue(matched)
        self.assertIsNone(diff)

    def test_compare_normalizes_nested_wkt_type_spacing(self):
        matched, diff = compare_result_rows(
            [("GEOMETRYCOLLECTION(POINT(-75 40),LINESTRING(-75 40,-76 41))",)],
            [("GEOMETRYCOLLECTION (POINT (-75 40),LINESTRING (-75 40,-76 41))",)],
            left_name="predicted",
            right_name="gold",
        )

        self.assertTrue(matched)
        self.assertIsNone(diff)

    def test_compare_treats_ewkb_hex_and_wkt_as_equivalent(self):
        matched, diff = compare_result_rows(
            [("0101000020E6100000811F7AF76CC852C07CF72235481D4440",)],
            [("POINT(-75.1316508 40.2287661)",)],
            left_name="gold",
            right_name="predicted",
        )

        self.assertTrue(matched)
        self.assertIsNone(diff)

    def test_compare_normalizes_wkt_separator_whitespace(self):
        matched, diff = compare_result_rows(
            [("LINESTRING(-75 40,-76 41)",)],
            [("LINESTRING (-75 40, -76 41)",)],
            left_name="predicted",
            right_name="gold",
        )

        self.assertTrue(matched)
        self.assertIsNone(diff)

    def test_non_wkt_strings_are_left_unchanged(self):
        self.assertEqual(
            normalize_execution_value("Valero POINT (store)"),
            "Valero POINT (store)",
        )

    def test_compare_treats_small_float_differences_as_equivalent(self):
        matched, diff = compare_result_rows(
            [(2.413128302477176,)],
            [(2.413128302477182,)],
            left_name="predicted",
            right_name="gold",
        )

        self.assertTrue(matched)
        self.assertIsNone(diff)

    def test_compare_keeps_large_float_differences_as_mismatch(self):
        matched, diff = compare_result_rows(
            [(2.413128302477176,)],
            [(2.413130302477182,)],
            left_name="predicted",
            right_name="gold",
        )

        self.assertFalse(matched)
        self.assertEqual(
            diff,
            {
                "only_in_predicted": [[2.413128302477176]],
                "only_in_gold": [[2.413130302477182]],
            },
        )

    def test_compare_applies_float_tolerance_inside_unordered_rows(self):
        matched, diff = compare_result_rows(
            [("a", 1.0000004), ("b", 2.0)],
            [("b", 2.0000003), ("a", 1.0)],
            left_name="predicted",
            right_name="gold",
        )

        self.assertTrue(matched)
        self.assertIsNone(diff)


if __name__ == "__main__":
    unittest.main()
