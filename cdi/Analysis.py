"""
This script contains the necessary functions to deal with the data, obtain data frame and show some graphics
"""

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JError
import pandas as pb


def get_data_frame_count_type_of_topic(data_frame: DataFrame) -> pb.DataFrame:
    """
    From all the data, it takes the columns TopicID and Question and for each topic, count the number of+
    different SubTopic/Question
    :param data_frame: generate with pyspark, and contain all the data from the csv file
    :return: data frame of panda package
    """
    try:
        data_frame = data_frame \
            .select("TopicID", "Question") \
            .distinct() \
            .groupBy("TopicID") \
            .count() \
            .sort("TopicID")
    except Py4JError:
        raise AnalysisException('One columns is incorrect')
    print("The following table represent the number of the type of each topic")
    data_frame.show()
    data_frame_pandas = data_frame.toPandas()
    return data_frame_pandas


def get_rdd_count_type_of_topy(rdd: list) -> pb.DataFrame:
    """
    Take an specific list from rdd spark, which is formed as list of tuples (Topic, Question)
    :param rdd: list of tuples(Topic, Question)
    :return: data frame of package Pandas
    """
    data_frame_pandas = pb.DataFrame(rdd, columns=['Topic', 'Question'])
    print(data_frame_pandas)
    return data_frame_pandas


def get_data_frame_count_male_gender_by_topic(data_frame: DataFrame) -> pb.DataFrame:
    """
    From all the data, it takes the columns TopicID, and count the topic based on the gender
    :param data_frame: generate with pyspark, and contain all the data from the csv file
    :return: data frame of panda package
    """
    data_frame_topic = data_frame \
        .filter(data_frame["Stratification1"].contains("Male")) \
        .distinct() \
        .groupBy("TopicID") \
        .count() \
        .sort("TopicID")

    print("The following table represent the number of men group by the topic: ")
    data_frame_topic.show()
    data_frame_pandas = data_frame.toPandas()
    return data_frame_pandas


def get_data_frame_count_black_ethnicity_by_topic(data_frame: DataFrame) -> pb.DataFrame:
    """
    From all the data, it takes the columns TopicID, and count the topic based on the ethnicity
    :param data_frame: generate with pyspark, and contain all the data from the csv file
    :return: data frame of panda package
    """
    data_frame_topic = data_frame \
        .filter(data_frame["Stratification1"].contains("Black, non-Hispanic")) \
        .distinct() \
        .groupBy("TopicID") \
        .count() \
        .sort("TopicID")

    print("The following table represent the number of black ethnicity people group by the topic: ")
    data_frame_topic.show()
    data_frame_pandas = data_frame.toPandas()
    return data_frame_pandas


def plot_type_of_topic(data_frame: pb.DataFrame) -> None:
    """
    Plot a data frame with bar type
    :param data_frame:
    :return:
    """
    plt.interactive(False)
    plt.figure()
    data_frame.plot(kind='bar', x= data_frame['TopicID'])
    plt.show()


def plot_type_of_two_topic(data_frame1: pb.DataFrame, data_frame2: pb.DataFrame) -> None:
    """
    Plot a data frame with bar type
    :param data_frame:
    :return:
    """
    plt.interactive(False)
    plt.figure()
    data_frame1.plot(kind='bar', x= data_frame['TopicID'])
    data_frame2.plot(kind='bar', x= data_frame['TopicID'])
    plt.show()





