import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from utilidades.utils_training import Utils
from constants.constants import Constants as c


class Testing(Utils):
    def __init__(self):
        self.CONS_EMTPY_SCHEMA = StructType([])
        pass


def get_some_functions_with_rdd(spark):
    """
    Mostrar algunos de las operaciones que se puede realizar con RDD
    :param spark:
    :return:
    """
    # Crear sc context
    sc = spark.sparkContext
    # Ejemplo 1 Crear un RDD apartir de una lista de los primeros 5 numeros
    rdd = sc.parallelize([1, 2, 3, 4, 5])
    rdd_salida = rdd.foreach(print)
    # Funcion map()
    rdd_cuadrado = rdd.map(lambda x: (x, x ** 2))
    rdd_cuadrado_salida = rdd_cuadrado.foreach(print)
    # Funcion collect()
    rdd_cuadrado.collect()
    # Funcion flatMap()
    rdd_cuadrado_flat = rdd.flatMap(lambda x: (x, x ** 2))
    rdd_cuadrado_flat.collect()
    rdd_cuadrado_salida_flat = rdd_cuadrado_flat.foreach(print)

    # Ejemplo 2
    rdd_texto = sc.parallelize(['Daniel', 'Diego', 'Lucy', 'Mary'])
    rdd_mayuscula = rdd_texto.flatMap(lambda x: (x, x.upper()))
    rdd_mayuscula.collect()
    rdd_nombres = rdd_mayuscula.foreach(print)

    # Ejemplo 3

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    # Funcion Filter()
    rdd_par = rdd.filter(lambda x: x % 2 == 0)
    print("-- Filter() Numeros pares ")
    rdd_par_salida = rdd_par.foreach(print)
    rdd_par.collect()
    # Otro ejemplo-
    print("-- Filter() Numeros impares ")
    rdd_impar = rdd.filter(lambda x: x % 2 != 0)
    rdd_impar.collect()
    rdd_impar_salida=rdd_impar.foreach(print)
    rdd_texto = sc.parallelize(['jose', 'joaquin', 'juan', 'lucia', 'karla', 'katia', 'juancho'])
    # Ejemplo
    print("-- Filter() startswith ='k' ")
    rdd_k = rdd_texto.filter(lambda x: x.startswith('k'))
    rdd_k.collect()
    rdd_k_salida = rdd_k.foreach(print)
    print("-- Filter() startswith ='j' and  find('u')")
    rdd_filtro = rdd_texto.filter(lambda x: x.startswith('j') and x.find('u') == 1)
    rdd_filtro.collect()
    rdd_filtro_sal = rdd_filtro.foreach(print)


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
    # Ejercion 2
    get_some_functions_with_rdd(spark)

    spark.stop
    print("Process Finished")


if __name__ == '__main__':
    main()
