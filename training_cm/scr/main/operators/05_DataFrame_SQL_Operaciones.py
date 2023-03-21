import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from utilidades.utils_training import Utils
from constants.constants import Constants as c


class Testing(Utils):
    def __init__(self):
        self.CONS_EMTPY_SCHEMA = StructType([])
        pass


def call_process(spark, path_file_simple_delimitado):
    """
    :param spark_session:
    :param path_file_simple_delimitado:
    :return:
    """
    """
    EJERCICIO: CREAR UN DATAFRAME CON DATOS DE UN CSV DELIMITADO, DE UNA MUESTRA DE LOS
    SOBREVIVIENTES DEL TITANIC
    1.- Crear el SparkSession
    2.- Pasar por parametro el path del archivo csv para leerlo desde spark
    3.- El archivo tiene encabezado, por lo cual se debe respetar y esta delimitado por la coma
    4.- Imprimir el Schema del dataframe.
    5.- Mostrar 50 registros de nuestro dataframe creado.
    """
    #3.- El archivo tiene encabezado, por lo cual se debe respetar y esta delimitado por la coma
    df_from_txt_delimited = Utils.read_csv_files_delimited(spark, path_file_simple_delimitado,
                                                           c.DELIMITED_COMMA)
    #4.- Imprimir el Schema del dataframe.
    df_eschema = df_from_txt_delimited.printSchema()
    #5.- Mostrar 50 registros de nuestro dataframe creado.
    df_from_txt_delimited.show(10, False)


def main():
    """
    :param params:
    :return:
    """
    print(c.DISPLAY_TXT + "Argumentos recibidos: " + str(sys.argv[1:]))
    #  1.- Crear el SparkSession
    spark = Utils.init_spark()
    # Parametros a utilizar en nuestro programa
    params = sys.argv[1:]
    # 2.- Pasar por parametro el path del archivo csv para leerlo desde spark
    path_file_simple = params[0]
    # Testing spark...
    print("> La sesion de spark es: {}".format(spark))
    call_process(spark, path_file_simple)


if __name__ == '__main__':
    main()
