import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from constants.constants import Constants as c


class Utils(object):
    @staticmethod
    def init_spark():
        print("Starting SparkSession ....")
        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("Testing Spark") \
            .config("spark.driver.bindAddress", "192.168.0.30") \
            .getOrCreate()
        print("Created SparkSession....")
        return spark

    @staticmethod
    def read_text_files(spark, path):
        print("> Leyendo archivo {}.... ".format(path))
        try:
            df = spark.read.text(path)
            print("> Procesado.. {}".format(path))
            return df
        except pyspark.sql.utils.AnalysisException as ex: \
                print("> Ha ocurrido un error, Por favor revisar acerca de ... {} ".format(ex))
        empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), c.EMTPY_SCHEMA)
        return empty_df

    @staticmethod
    def read_csv_files_simple(spark, path):
        """
        :param spark:
        :param path:
        :return:
        """
        print("> Leyendo archivo {}.... ".format(path))
        try:
            df = spark.read.csv(path)
            print("> Procesado.. {}".format(path))
            return df
        except pyspark.sql.utils.AnalysisException as ex: \
                print("> Ha ocurrido un error, Por favor revisar acerca de ... {} ".format(ex))
        empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), c.EMTPY_SCHEMA)
        return empty_df

    @staticmethod
    def read_csv_files_delimited(spark, path, delimiter):
        """
        :param spark:
        :param path:
        :param delimiter:
        :return:
        """
        print("> Leyendo archivo {}, con el delimitador {} .... ".format(path, delimiter))
        try:
            df = spark.read.format("csv") \
                .option('header', 'true') \
                .option('delimiter', delimiter) \
                .option('inferSchema', "true") \
                .load(path)
            print("> Procesado.. {}".format(path))
            return df
        except pyspark.sql.utils.AnalysisException as ex: \
                print("> Ha ocurrido un error, Por favor revisar acerca de ... {} ".format(ex))
        empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), c.EMTPY_SCHEMA)
        return empty_df

    @staticmethod
    def read_json_files(spark, json_path, json_schema):
        """
        :param spark:
        :param path:
        :param delimiter:
        :return:
        """
        print("> Leyendo archivo {} .... ".format(json_path))
        try:
            df = spark.read.schema(json_schema).json(json_path)
            print("> Procesado.. {}".format(json_path))
            return df
        except pyspark.sql.utils.AnalysisException as ex: \
                print("> Ha ocurrido un error, Por favor revisar acerca de ... {} ".format(ex))
        empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), c.EMTPY_SCHEMA)
        return empty_df

    @staticmethod
    def read_parquet_files(spark, parquet_path):
        """
        :param spark:
        :param path:
        :param delimiter:
        :return:
        """
        print("> Leyendo archivo {} .... ".format(parquet_path))
        try:
            df = spark.read.parquet(parquet_path)
            print("> Procesado.. {}".format(parquet_path))
            return df
        except pyspark.sql.utils.AnalysisException as ex: \
                print("> Ha ocurrido un error, Por favor revisar acerca de ... {} ".format(ex))
        empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), c.EMTPY_SCHEMA)
        return empty_df
