import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from utilidades.utils_training import Utils


class Testing(Utils):
    def __init__(self):
        self.CONS_EMTPY_SCHEMA = StructType([])
        pass

def init_spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Testing Spark") \
        .config("spark.driver.bindAddress", "192.168.0.30") \
        .getOrCreate()
    return spark


def get_hello_world(spark):
    #
    df = spark.createDataFrame([{"Hello": "World"} for x in range(10)])
    df.show(10, False)


def main():
    """
    :param params:
    :return:
    """
    print(str(sys.argv[1:]))
    params = sys.argv[1:]
    path_file_txt = params[0]
    spark_session = init_spark()
    print("Argumento recibido: {}".format(path_file_txt))
    #Leer un archivo de texto plano
    get_hello_world(spark_session)
    #Lectura de un archivo.
    #Practicar, pasarle por parametro la ubicacion del archivo hello_rdd_source.txt
    txt_df = spark_session.read.text(path_file_txt)\
        .show(10, False)
    spark_session.stop
    print("sparkSession Stopped.")
    print("Process Finished.")



if __name__ == '__main__':
    main()
