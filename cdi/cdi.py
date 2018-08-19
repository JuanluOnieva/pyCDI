import ReadCSV
import Analysis


def main() -> None:
    #file_csv = 'data/Chronic_Disease_Indicators_CDI.csv'
    file_csv = 'cdi/data/cdi.csv'

    # Mandatory Analysis
    data_frame = ReadCSV.read_csv_with_data_frame(file_csv)
    rdd = ReadCSV.read_csv_with_rdd(file_csv)
    data_frame_count_type_DF = Analysis.get_data_frame_count_type_of_topic(data_frame)
    data_frame_count_type_RDD = Analysis.get_rdd_count_type_of_topy(rdd)
    Analysis.plot_type_of_topic(data_frame_count_type_DF)
    Analysis.plot_type_of_topic(data_frame_count_type_RDD)

    # Optional Analysis
    data_frame_count_men = Analysis.get_data_frame_count_male_gender_by_topic(data_frame)
    data_frame_count_black_ethnicity = Analysis.get_data_frame_count_black_ethnicity_by_topic(data_frame)
    Analysis.plot_type_of_topic(data_frame_count_men)
    Analysis.plot_type_of_topic(data_frame_count_black_ethnicity)

    print("Proccess finished correctly")


if __name__ == '__main__':
    main()