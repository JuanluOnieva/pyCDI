import unittest
from ReadCSV import read_csv_with_data_frame
from Analysis import get_data_frame_count_type_of_topic
from pyspark.sql.utils import AnalysisException

"""This tests the functions from Analysis.py."""


class MyTestCase(unittest.TestCase):

    def setUp(self):
        """
        Defining the initial conditions for the rest of cases to test
        """
        self.data_frame = read_csv_with_data_frame('data/pruebas.csv')
        # This is the correct file
        self.data_frame_wrong = read_csv_with_data_frame('data/pruebas-wrong-column.csv')
        # This file contains the wrong amount of columns

    def test_when_count_subtopic_data_frame_should_have_at_least_columns_with_topic_and_subtopic(self):
        """
        This tries the get_data_frame_count_type_of_topic() function, raising an exception upon a wrong amount of
         columns.
        """
        with self.assertRaises(AnalysisException):
            get_data_frame_count_type_of_topic(self.data_frame_wrong)

    def test_the_number_of_topic_must_be_correct(self):
        """
        This ensures it counts well the amount of topics read
        """
        data_frame_topic = get_data_frame_count_type_of_topic(self.data_frame)
        total = data_frame_topic.count()
        expected_value = 3
        self.assertEqual(expected_value, total)

    def test_the_total_number_must_correspond_with_size_of_csv(self):
        """
        This ensures the number of different topics read are 6
        """
        data_frame_topic = get_data_frame_count_type_of_topic(self.data_frame)
        data_frame_pandas = data_frame_topic.toPandas()
        total = sum(data_frame_pandas['count'])
        expected_value = 6
        self.assertEqual(expected_value, total)



if __name__ == '__main__':
    unittest.main()
