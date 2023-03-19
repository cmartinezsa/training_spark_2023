import pyspark
from pyspark.sql.types import StructType


class Utils(object):
    CONS_EMTPY_SCHEMA = StructType([])

    @staticmethod
    def read_text_files(spark, path):
        print("Leyendo archivo {}.... ".format(path))
        try:
            df = spark.read.text(path)
            return df
        except pyspark.sql.utils.AnalysisException as ex: \
                print("Ha ocurrido un error, revisar acerca de ... {} ".format(ex))
        empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), Utils.CONS_EMTPY_SCHEMA)
        return empty_df
