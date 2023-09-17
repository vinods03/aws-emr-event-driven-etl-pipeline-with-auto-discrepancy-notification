from util import get_spark_session, get_secret
from pyspark.sql.functions import col, explode, year, month, dayofmonth, hour

appName = 'orders-s3-landing-to-s3-staging'
redshift_url = "jdbc:redshift://ecommerce-redshift-cluster.cal5w1bhifg1.us-east-1.redshift.amazonaws.com:5439/dev"

def core():

    spark = get_spark_session(appName)
    dt = spark.sql('select current_date()')
    dt.show()

    emr_credentials = get_secret()
    print('emr_credentials: ', emr_credentials)
    print('type of emr_credentials: ', type(emr_credentials))

    emr_user = emr_credentials['emr_user']
    emr_password = emr_credentials['emr_password']

    # using s3 as the data source
    # source_df = spark.read.parquet('s3://fo-to-dw-orders-landing-area/orders/')
    # source_df.show()
    # print('Number of records in source df is: ', source_df.count())
    
    # using glue data catalog and redshift as the source
    # we want to use this approach instead of s3 so that the bookmarking will be easier using run_id
    # first run goes to the exception block because staging table will not exist during the first run
    # in the subsequent runs, we need to process only newer data -> this is the equivalent of Glue job bookmark in EMR

    try:
        redshift_staging_df = spark.read \
        .format("jdbc") \
        .option("url", redshift_url) \
        .option("dbtable", "ecommerce_staging.orders") \
        .option("user", emr_user) \
        .option("password", emr_password) \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .load()

        redshift_staging_df.createOrReplaceTempView('redshift_staging_tbl')

        source_df = spark.sql("select * from `ecommerce-database`.`staging_orders` where run_id not in (select distinct run_id from redshift_staging_tbl)");
    
    except Exception as e:

        source_df = spark.sql("select * from `ecommerce-database`.`staging_orders`");

    
    source_df.show()

    final_df = source_df.select('run_id', 'order_id', 'customer_id', 'seller_id', 'product_code', 'product_name', 'product_price', 'product_qty', 'order_purchase_timestamp', 'processing_timestamp')
    
    final_df.write.format('jdbc').options(
      url=redshift_url,	  
      driver='com.amazon.redshift.jdbc42.Driver',
      dbtable='ecommerce_staging.orders',
      user=emr_user,
      password=emr_password).mode('append').save() 

if __name__ == '__main__':
    core()
