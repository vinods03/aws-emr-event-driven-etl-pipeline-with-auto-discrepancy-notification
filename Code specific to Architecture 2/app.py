from util import get_spark_session
from pyspark.sql.functions import col, explode, year, month, dayofmonth, hour

appName = 'orders-s3-landing-to-s3-staging'

def core():

    spark = get_spark_session(appName)
    dt = spark.sql('select current_date()')
    dt.show()

    # using s3 as the data source
    # source_df = spark.read.parquet('s3://fo-to-dw-orders-landing-area/orders/')
    # source_df.show()
    # print('Number of records in source df is: ', source_df.count())
    
    # using glue data catalog as the source
    # we want to use glue data catalog instead of s3 so that the bookmarking will be easier using the Glue data catalog (run_id) approach
    # first run goes to the exception block because staging table will not exist during the first run
    # in the subsequent runs, we need to process only newer data -> this is the equivalent of Glue job bookmark in EMR
    
    try:
        source_df = spark.sql("select * from `ecommerce-database`.`orders` where run_id not in (select distinct run_id from `ecommerce-database`.`staging_orders`)");
    except Exception as e:
        source_df = spark.sql("select * from `ecommerce-database`.`orders`");

    # source_df = spark.sql("SELECT * FROM `ecommerce-database`.orders")
    
    source_df.show()

    df2 = source_df.withColumn('exploded_products', explode('products'))
    
    df3 = df2.withColumn('product_code', col('exploded_products.product_code')) \
             .withColumn('product_name', col('exploded_products.product_name')) \
             .withColumn('product_price',col('exploded_products.product_price')) \
             .withColumn('product_qty', col('exploded_products.product_qty'))
    
    # Below code is needed when you read from S3.
    # The partition columns will not be available and you would have to derive these
    # But when you fread from Glue Data Catalog this is not needed
    
    # df4 = df3.withColumn('year', year(col('processing_timestamp'))) \
    #          .withColumn('month', month(col('processing_timestamp'))) \
    #          .withColumn('day', dayofmonth(col('processing_timestamp'))) \
    #          .withColumn('hour', hour(col('processing_timestamp'))) \
             
    df5 = df3.select('run_id', 'order_id', 'customer_id', 'seller_id', 'product_code', 'product_name', 'product_price', 'product_qty', 'order_purchase_timestamp', 'processing_timestamp', 'year', 'month', 'day', 'hour')

    df5.show()

    df5.write.partitionBy('year', 'month', 'day', 'hour').mode('append').save('s3://fo-to-dw-orders-staging-area/orders/')

if __name__ == '__main__':
    core()
