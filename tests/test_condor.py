from tuna.condor import CondorJobRunner, _build_arg_str
import unittest
from unittest.mock import patch

class TestCondor(unittest.TestCase):
    def test_escape(self):
        self.assertEqual(
            _build_arg_str(["3", "simple", "arguments"]),
            "\"3 simple arguments\""
        )

        self.assertEqual(
            _build_arg_str(["one", "two with spaces", "3"]),
            "\"one 'two with spaces' 3\""
        )

        self.assertEqual(
            _build_arg_str(["one", "\"two\"", "spacey 'quoted' argument"]),
            "\"one \"\"two\"\" 'spacey ''quoted'' argument'\""
        )

        



if __name__ == "__main__":
    unittest.main()