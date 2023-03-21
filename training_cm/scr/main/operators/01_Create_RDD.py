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
    sc = spark_session.sparkContext
    # Testing spark...
    print("> La sesion de spark es: {}".format(spark_session))
    print(c.DISPLAY_TXT_DECORED + "EJEMPLO CREACION DE RDD DEL 1 AL 5 ")
    # Creacion un RDD
    rdd_num = sc.parallelize([1, 2, 3, 4, 5])
    rdd_num.collect()
    rdd_num.foreach(print)

    # Podemos realizar operaciones con los RDD usando la funcion map()
    print(c.DISPLAY_TXT_DECORED + "EJEMPLO CREACION DE RDD  BASADO EN EL ANTERIOR * 2")
    rdd_suma = rdd_num.map(lambda x: (str(x) + ' * 2 = ' + str(x * 2)))
    rdd_suma.collect()
    rdd_suma.foreach(print)

    # Crear un RDD desde un archivo de texto
    print(c.DISPLAY_TXT_DECORED + "EJEMPLO CREACION DE RDD DESDE TEXTO")
    rdd_texto = sc.textFile(path_file_simple)
    rdd_texto.collect()
    rdd_texto.foreach(print)

    print(c.DISPLAY_TXT_DECORED + "EJEMPLO CREACION DE RDD DESDE TEXTO COMPLETO")
    rdd_texto_completo = sc.wholeTextFiles(path_file_simple)
    rdd_texto_completo.collect()
    rdd_texto_completo.foreach(print)

    print(c.DISPLAY_TXT_DECORED + "CONVERTIR UN DATAFRAME A RDD ")
    df = spark_session.createDataFrame([(1, 'jose'), (2, 'juan')], ['id', 'nombre'])
    df.show()
    rdd_df = df.rdd
    rdd_df.collect()

    print(c.DISPLAY_TXT_DECORED + " CONVERTIR UN RDD A DATAFRAME")
    dept = [("Finance", 10), \
            ("Marketing", 20), \
            ("Sales", 30), \
            ("IT", 40) \
            ]
    rdd = spark_session.sparkContext.parallelize(dept)
    dataColl = rdd.collect()
    #Imprimir los valores del un RDD
    for row in dataColl:
        print(row[0] + "," + str(row[1]))

    rdd_to_df = rdd.toDF(c.RRD_TO_DF_SCHEMA).show(10, False)

    # Otro Ejemplo de como generar un rdd y convertirlo a dataframe.

    rdd = sc.parallelize([item for item in range(10)]).map(lambda x: (x, x ** 2))
    rdd.collect()
    df = rdd.toDF(['numero', 'cudrado'])
    df.printSchema()
    df.show()

def get_some_functions_with_rdd(spark):
    """
    Mostrar algunos de las operaciones que se puede realizar con RDD
    :param spark:
    :return:
    """
    #Crear sc context
    sc = spark.sparkContext
    #Ejemplo 1 Crear un RDD apartir de una lista de los primeros 5 numeros
    rdd = sc.parallelize([1, 2, 3, 4, 5])
    # Funcion map()
    rdd_cuadrado = rdd.map(lambda x: (x, x ** 2))
    #Funcion collect()
    rdd_cuadrado.collect()
    #Funcion flatMap()
    rdd_cuadrado_flat = rdd.flatMap(lambda x: (x, x ** 2))
    rdd_cuadrado_flat.collect()

    #Ejemplo 2
    rdd_texto = sc.parallelize(['jose', 'juan', 'lucia'])
    rdd_mayuscula = rdd_texto.flatMap(lambda x: (x, x.upper()))
    rdd_mayuscula.collect()


def init_spark():
    print("Starting SparkSession ....")
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Testing Spark") \
        .config("spark.driver.bindAddress", "192.168.100.90") \
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
    spark = init_spark()
    # Llamar funcion que gestiona nuestro programa...

    #Ejercion 1
    #call_functions(spark_session, path_file_simple)

    #Ejercion 2
    get_some_functions_with_rdd(spark)
    spark.stop
    print("Process Finished")


if __name__ == '__main__':
    main()
