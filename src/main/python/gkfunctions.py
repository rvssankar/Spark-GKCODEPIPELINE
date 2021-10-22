# All the neccesary functions:

from pyspark.sql.types import StructType,StructField, StringType,IntegerType,TimestampType,DoubleType

def read_schema(schemaValue):
    dtypes ={
        "StringType()": StringType(),
        "IntegerType()": IntegerType(),
        "TimestampType()": TimestampType(),
        "DoubleType()": DoubleType()
    }
    splitvalue = schemaValue.split(",")

    sch = StructType()

    for i in splitvalue:
        x = i.split(" ")
        sch.add(StructField(x[0], dtypes[x[1]], True))

    return sch
