//prueba joses 2
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from utilidades.utils_training import Utils
from constants.constants import Constants as c


class Testing(Utils):
    def __init__(self):
        self.CONS_EMTPY_SCHEMA = StructType([])
        pass

def call_functions(spark_session, path_file_simple):
    """
    :param spark_session:
    :param path_file_simple:
    :return:
    """
    """
    Ejercicio
    1.- Crear el SparkSession
    2.- Crear SContext
    3.- Crear un RDD con numeros del 1 a 50
    4.- Imprimir un RDD 
    5.- Convertir el RRD anterior a un DataFrame
    6.- Imprimir el schema del nuevo DataFrame
    7.- Mostrar 30 registros del DataFrame.
    - 
    """
    # 2.- Crear SContext
    sc = spark_session.sparkContext
    # 3.- Crear un RDD con numeros del 1 a 50
    num= list(range(0,50))
    print(num)
    rdd = sc.parallelize(num).map(lambda x: (x, x ** 2))
    # 4.- Imprimir un RDD
    rdd_inmutable=rdd.foreach(print)
    # rdd = sc.parallelize([item for item in range(50)]).map(lambda x: (x, x ** 2))
    rdd.collect()
    # 5.- Convertir el RRD anterior a un DataFrame
    df = rdd.toDF(['numero', 'cudrado'])
    # 6.- Imprimir el schema del nuevo DataFrame
    df.printSchema()
    # 7.- Mostrar 30 registros del DataFrame.
    df.show(30,False)

def init_spark():
    print("Starting SparkSession ....")
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Testing Spark") \
        .config("spark.driver.bindAddress", "192.168.0.30") \
        .getOrCreate()
    print("Created SparkSession....")
    return spark


def main():
    """
    :param params:
    :return:
    """
    print("Starting Process ....")
    print(c.DISPLAY_TXT + "Argumentos recibidos: " + str(sys.argv[1:]))
    # Parametros a utilizar en nuestro programa
    params = sys.argv[1:]
    path_file_simple = params[0]
    spark_session = Utils.init_spark()
    # Llamar funcion que gestiona nuestro programa...
    call_functions(spark_session, path_file_simple)
    spark_session.stop
    print("Process Finished")


if __name__ == '__main__':
    main()
