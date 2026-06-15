import dlt
from pyspark.sql.functions import current_timestamp

@dlt.table(
    name="transactions"
)

def transactions():
    df=spark.readStream.table("dltsharannyo.banking_source.bank_transactions_raw")
    df=df.withColumn("ingestion_ts", current_timestamp())
    return df
