"""
This class tests the functions from ReadCSV.py
"""

import unittest
from ReadCSV import read_csv_with_data_frame
from pyspark.sql.utils import AnalysisException


class MyTestCase(unittest.TestCase):
    def test_read_csv_from_data_frame_read_correctly(self):
        """
        This ensures the file read has a size of 12 columns
        """
        data_frame = read_csv_with_data_frame('data/pruebas.csv')
        data_frame_total = data_frame \
            .count()
        expected_value = 12
        self.assertEqual(expected_value, data_frame_total)

    def test_raise_exception_when_the_file_is_not_csv(self):
        """
        This ensures it doesn't read a file that's not from a .csv file format, there is an exception
        """
        with self.assertRaises(AnalysisException):
            read_csv_with_data_frame('data/pruebas.tsv')

    def test_raise_exception_when_the_file_not_exist(self):
        """
        This ensures if it tries to read a file that doesn't exist or that's not found, there is an exception
        """
        with self.assertRaises(AnalysisException):
            read_csv_with_data_frame('data/no-file.tsv')


if __name__ == '__main__':
    unittest.main()
