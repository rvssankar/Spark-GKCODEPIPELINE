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
productFileSchemaConf = config.get('schemas','ProductFileSchema')

landingFileSchema = read_schema(fileSchema)
productFileSchema = read_schema(productFileSchemaConf)

# Handling Dates:
today = datetime.now()
yesterday = today - timedelta(1)

# currentDaySuffix = "_" + today.strftime("%d%m%Y")
# previousDaySuffix = "_" + yesterday.strftime("%d%m%Y")

currentDaySuffix = "_22102021"
previousDaySuffix = "_21102021"

# Read data from Valid file:

validDf = spark.read.schema(landingFileSchema)\
    .option('delimiter','|')\
    .option('header',True)\
    .csv(outputLocation + "Valid/ValidData" + currentDaySuffix)

validDf.createOrReplaceTempView('validDf')

# Read data from Product file:

productEnrichedDF = spark.read.schema(productFileSchema)\
    .option('delimiter','|')\
    .option('header',True)\
    .csv(inputLocation + 'Products/GKProductList.dat')


productEnrichedDF.createOrReplaceTempView('productEnrichedDF')

enrichedProductDF = spark.sql("SELECT a.Sale_ID, a.Product_ID, b.Product_Name, a.Quantity_Sold, "
                              "a.Vendor_ID,a.Sale_Date, "
                              "b.Product_Price * a.Quantity_Sold as Sale_Amount, a.Sale_Currency "
                              "FROM validDf a INNER JOIN productEnrichedDF b "
                              "ON a.Product_ID = b.Product_ID")


# Write the Data to Enriched folder:

enrichedProductDF.write\
    .option('delimiter','|')\
    .option('header',True)\
    .mode('overwrite')\
    .csv(outputLocation + 'Enriched/SaleAmountEnrichment/SaleAmountEnrichment' + currentDaySuffix)

print("Data loaded successfully")