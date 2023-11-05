from pyspark.sql.types import StructType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

#clase de constantes que se utilizan en el proyecto
class Constants:
    EMTPY_SCHEMA = StructType([])
    FORMAT_DATE_SIMPLE = 'yyyy-MM-dd'
    DISPLAY_TXT = "> "
    DELIMITED_C = 'Ç'
    DELIMITED_COMMA = ','
    DELIMITED_PIPE = ','
    DISPLAY_TXT_DECORED = " ******* "

    #esquema tabla personas
    JSON_SCHEMA_PEOPLE = StructType(
        [
            StructField('color', StringType(), True),
            StructField('edad', IntegerType(), True),
            StructField('fecha', DateType(), True),
            StructField('pais', StringType(), True)
        ]
    )

    RRD_TO_DF_SCHEMA = StructType(
        [
            StructField('depto', StringType(), True),
            
        ]
    )
