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
enrichedfileSchemaConf = config.get('schemas','ProductEnrichmentSchema')
vendorFileSchemaConf = config.get('schemas','vendorSchema')
USDFileSchemaConf = config.get('schemas','usdreferenceSchema')

enrichFileSchema = read_schema(enrichedfileSchemaConf)
vendorFileSchema = read_schema(vendorFileSchemaConf)
USDFileSchema = read_schema(USDFileSchemaConf)

# Handling Dates:
today = datetime.now()
yesterday = today - timedelta(1)

# currentDaySuffix = "_" + today.strftime("%d%m%Y")
# previousDaySuffix = "_" + yesterday.strftime("%d%m%Y")

currentDaySuffix = "_22102021"
previousDaySuffix = "_21102021"

# Read data from Enriched Product file:

EnrichedProductDF = spark.read.schema(enrichFileSchema)\
    .option('delimiter','|')\
    .option('header',True)\
    .csv(outputLocation + 'Enriched/SaleAmountEnrichment/SaleAmountEnrichment' + currentDaySuffix)

EnrichedProductDF.createOrReplaceTempView('EnrichedProductDF')

# Read data from Vendor file:

VendorDF = spark.read.schema(vendorFileSchema)\
    .option('delimiter','|')\
    .option('header',False)\
    .csv(inputLocation + 'Vendors/vendor_dump.dat')

VendorDF.createOrReplaceTempView('VendorDF')

# Read data from USD exchange Rate file:

USDRateDF = spark.read.schema(USDFileSchema)\
    .option('delimiter','|')\
    .option('header',False)\
    .csv(inputLocation + 'USD_Rates/USD_Rates_Reference.dat')

USDRateDF.createOrReplaceTempView('USDRateDF')

# Main Dataframe create

VendorEnrichedDF = spark.sql("SELECT a.*, b.Vendor_Name "
                             "FROM EnrichedProductDF a INNER JOIN VendorDF b "
                             "ON a.Vendor_ID = b.Vendor_ID")
VendorEnrichedDF.createOrReplaceTempView('VendorEnrichedDF')

usdEnrichedDF = spark.sql("SELECT a.* , ROUND((a.Sale_Amount/b.Exchange_Rate),2) as Sale_Amount_USD "
                          "FROM VendorEnrichedDF a INNER JOIN USDRateDF b "
                          "ON a.Sale_Currency = b.Currency_Code")

#Write the Data to file:

usdEnrichedDF.write.mode('overwrite')\
    .option('delimiter','|')\
    .option('header',True)\
    .csv(outputLocation + 'Enriched\Vendor_USD_Enriched/Vendor_USD_Enriched'+ currentDaySuffix)

print('Data loaded Successfully into file')

# Write the data into MySQL database:

usdEnrichedDF.write.mode('append')\
    .format('jdbc')\
    .options(
    url = 'jdbc:mysql://localhost:3306/gkstorepipelinedb',
    driver ='com.mysql.jdbc.Driver',
    dbtable ='finaltable',
    user ='root',
    password = 'root'
)\
    .save()


print('Data loaded Successfully into Database')