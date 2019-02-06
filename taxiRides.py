import pyspark
from pyspark.sql import functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp,udf,hour,minute,from_unixtime,date_format,year,month

print('building spark session')
spark = SparkSession\
    .builder\
    .appName("NYTaxiRides2Paquet")\
    .getOrCreate()

def createParquet():

    try:
        #reading csv files
        print('Reading data files')
        df = spark.read.option("header","true")\
            .option("inferschema","true")\
            .csv("/home/jovyan/2017_Yellow_Taxi_Trip_Data.csv")
        
        print('Dataframe has been created successfully')
        try:
           #creating parquet files
           print('Iniciando a criacao de arquivos em parquet')
           df.write.parquet('/home/jovyan/NY_TAXI_RIDES')#partitionBy("Year","Month")\
               
           print('')
           print('End of pipeline')
        except:
            print('Erro ao escrever arquivo')
            raise
        
        #excluindo corridas zeradas
        #df = df[(df['fare_amount'] > 0)\
        #        | (df['total_amount'] > 0 )]
    except IOError as e:
        print("Erro ao ler o arquivo")
        raise


def payment_method():
    #creating payment method dimension
    dfPayment = spark.read.option("header","true")\
                .option("inferschema","true")\
                .parquet("/home/jovyan/NY_TAXI_RIDES")
    print('creating payment method dimension')
    dfPayment = dfPayment\
                .withColumn("Payment_des", functions\
                .when(dfPayment.payment_type == 1, 'CREDIT CARD')
                .when(dfPayment.payment_type == 2, 'CASH')
                .when(dfPayment.payment_type == 3, 'NO CHARGE')
                .when(dfPayment.payment_type == 4, 'DISPUTE')
                .when(dfPayment.payment_type == 5, 'UNKNOWN')
                .otherwise('VOIDED TRIP')
                )
    df_payment_type =  dfPayment.select("payment_type","Payment_des").distinct()
    df_payment_type.write.parquet('/home/jovyan/NY_TAXI_RIDES_'+'PAYMENT_TYPE')
          

if __name__ == '__main__':
    #Running the functions to create parquet files 
    createParquet()
    payment_method()
