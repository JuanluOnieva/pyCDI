"""
This script contains the function to read the csv with different methods
"""


from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JError
from pyspark import SparkConf, SparkContext
import os


def read_csv_with_data_frame(file_csv: str) -> DataFrame:
    """
    Read CSV with as data frame with spark
    :param file_csv: file name of csv
    :return: all the data of the file as data frame
    """
    spark_session = SparkSession \
        .builder \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    try:
        data_frame = spark_session\
            .read\
            .format("csv") \
            .options(header='true', inferschema='true')\
            .load(file_csv)
    except Py4JError:
        raise AnalysisException('There is no csv file in:'  + str(os.path))

    return data_frame


def read_csv_with_rdd(file_csv: str) -> SparkContext:
    """
    Read csv file with rdd, then take only the columns 5 (TopicID) and 6 (Question) and produce a list of tuples
    :param file_csv: file name of csv
    :return: list of tuples (TopicID, Question)s
    """
    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)
    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
    rdd = spark_context \
        .textFile(file_csv)
    header = rdd.first()
    rdd = rdd.filter(lambda row: row!=header) \
        .map(lambda line: line.split(",")) \
        .map(lambda line: (line[4], line[6])) \
        .distinct() \
        .map(lambda list: (list[0], 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda pair: pair[0]) \
        .collect()
    spark_context.stop()
    return rdd