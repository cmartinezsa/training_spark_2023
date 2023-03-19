import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from utilidades.utils_reader import Utils
from constants.constants import Constants as c


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


def main():
    """
    :param params:
    :return:
    """
    print(c.DISPLAY_TXT + "Argumentos recibidos: " + str(sys.argv[1:]))
    # Parametros a utilizar en nuestro programa
    params = sys.argv[1:]
    path_json_file = params[0]
    spark_session = init_spark()
    # Testing spark...
    print("> La sesion de spark es: {}".format(spark_session))

    print(c.DISPLAY_TXT_DECORED + "EJEMPLO DE CREAR UN DATAFRAME CON DATOS DE UN ARCHIVO JSON" + c.DISPLAY_TXT_DECORED)
    df_from_json_file = Utils.read_json_files(spark_session, path_json_file, c.JSON_SCHEMA_PEOPLE) \
        .show(10, False)


if __name__ == '__main__':
    main()
