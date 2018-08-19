# Indicators for Chronic Disease Surveillance

## Abstract
We present an analysis of data from U.S. Chronic Disease Indicators (CDI) an study from 2013 as well how to do this
analysis with dataframes and RDDs using python 3.

## Introduction
The files related to reading are on ReadCSV.py. Here you can find two functions. One will read a file and return a data
frame and the other will read it and return an rdd.

## Data Frame
On ReadCSV.py the function read_csv_with_data_frame(file_csv: str) -> DataFrame receiving a file will read it with spark
as a .csv with header. It will then return an object with all the information read from each column found as a data
frame. Data frames are pretty easy to work with, much better than RDDs as we'll see after this.

We work with this information on the file Analysis.py. Here we'll work with the methods
get_data_frame_count_type_of_topic(data_frame: DataFrame) -> DataFrame and
plot_type_of_topic(data_frame: DataFrame) -> None.
The first one given a data frame, it will separate from the data
frame "TopicID" and "Question" columns, group them by topic and count, so we get an amount of each topic.
The other one given a data frame it will plot it as a barplot.

## RDD

On ReadCSV.py the function read_csv_with_rdd(file_csv: str) -> SparkContext receiving a file will read it with spark but
we work with it on the same function. So after reading it, we save the first line, header, and remove it to work only
with pure data. Therefore, we begin the treatment as before to get the amount of different topics using lambda
expressions. We begin filtering the header out, then splitting by comma, we take the lines 5 and 6, which correspond to
the columns "TopicID" and "Question". We list each of them with 2 values: itself and one. Then we reduce by key, summing
each one that has the same key. Lastly we give its header back.

## Execute
To begin, execute cdi.py. It will take the data found in data/cdi.csv.
