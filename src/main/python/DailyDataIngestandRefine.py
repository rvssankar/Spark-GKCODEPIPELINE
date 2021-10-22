from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from src.main.python.gkfunctions import read_schema
import configparser
from datetime import datetime,date,time,timedelta

# Initiate the Spark Session:

spark = SparkSession.builder.appName("DataIngestandRefine").master("local").getOrCreate()

# Read from Config file:

config = configparser.ConfigParser()
config.read(r'..\projectconfigs\config.ini')
inputLocation = config.get('paths','inputLocation')
outputLocation = config.get('paths','outputLocation')
fileSchema = config.get('schemas','LandFileSchema')
holdFileSchemaconf = config.get('schemas','HoldFileSchema')

landingFileSchema = read_schema(fileSchema)
holdFileSchema = read_schema(holdFileSchemaconf)

# Handling Dates:
today = datetime.now()
yesterday = today - timedelta(1)

# currentDaySuffix = "_" + today.strftime("%d%m%Y")
# previousDaySuffix = "_" + yesterday.strftime("%d%m%Y")

currentDaySuffix = "_22102021"
previousDaySuffix = "_21102021"

# Previous Day Hold Data:

previousDayDataDF = spark.read \
    .schema(holdFileSchema)\
    .option('delimiter','|') \
    .option('header',True) \
    .csv(outputLocation + 'Hold/HoldData' + previousDaySuffix)


# Landing Zone:

landingFileDf = spark.read.\
    schema(landingFileSchema).\
    option("delimiter","|").\
    csv(inputLocation + "Sales_Landing/SalesDump" + currentDaySuffix)

# Create Table view for current and previous day:

previousDayDataDF.createOrReplaceTempView('previousDayView')
landingFileDf.createOrReplaceTempView('landingFileView')

# Join the table using SQL:

refreshedDataDF = spark.sql("SELECT a.Sale_ID,a.Product_ID,"
                            "CASE "
                            "WHEN (a.Quantity_Sold is null) THEN b.Quantity_Sold "
                            "ELSE a.Quantity_Sold "
                            "END as Quantity_Sold,"
                            "CASE "
                            "WHEN (a.Vendor_ID is null ) THEN b.Vendor_ID "
                            "ELSE a.Vendor_ID "
                            "END as Vendor_ID,"
                            "a.Sale_Date, "
                            "a.Sale_Amount, "
                            "a.Sale_Currency "
                            " from landingFileView a left outer join previousDayView b "
                            "ON a.Sale_ID = b.Sale_ID")

refreshedDataDF.createOrReplaceTempView("refreshedDataDF")

# This code is for join using Native functions:

joinDataDF = landingFileDf.join(previousDayDataDF,how='left', on='Sale_ID')\
    .select(landingFileDf.Sale_ID,landingFileDf.Product_ID,\
            when(landingFileDf.Quantity_Sold.isNull(),previousDayDataDF.Quantity_Sold)\
            .otherwise(landingFileDf.Quantity_Sold).alias('Quantity_Sold'), \
            when(landingFileDf.Vendor_ID.isNull(), previousDayDataDF.Vendor_ID) \
            .otherwise(landingFileDf.Vendor_ID).alias('Vendor_ID'),\
landingFileDf.Sale_Date,landingFileDf.Sale_Amount,landingFileDf.Sale_Currency
            )

# Check the Data:

validDf = refreshedDataDF.filter(col("Quantity_Sold").isNotNull() & col("Vendor_ID").isNotNull())

releasedFromHold = spark.sql("SELECT rdf.Sale_ID FROM refreshedDataDF rdf INNER JOIN previousDayView pv "
                             "ON rdf.Sale_ID = pv.Sale_ID")
releasedFromHold.createOrReplaceTempView("releasedFromHold")

notReleasedFromHold = spark.sql("SELECT * FROM previousDayView WHERE Sale_ID NOT IN "
                                "(SELECT Sale_ID from releasedFromHold) ")

invalidDf = refreshedDataDF.filter(col("Quantity_Sold").isNull() | col("Vendor_ID").isNull())\
    .withColumn("Hold_Reason",when(col('Quantity_Sold').isNull(),"Qty Sold Missing")\
                .otherwise(when(col("Vendor_ID").isNull(),"Vendor ID missing")))\
    .union(notReleasedFromHold)

# Writing the data:

validDf.write \
    .mode('overwrite')\
    .option('delimiter','|')\
    .option('header',True)\
    .csv(outputLocation + 'Valid/ValidData' + currentDaySuffix)

invalidDf.write \
    .mode('overwrite')\
    .option('delimiter','|')\
    .option('header',True)\
    .csv(outputLocation + 'Hold/HoldData' + currentDaySuffix)

print("Data successfully loaded")