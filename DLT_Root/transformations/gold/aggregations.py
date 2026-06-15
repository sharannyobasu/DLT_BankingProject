import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="txn_aggregations"
)

def txn_aggregations():
    df=dlt.read_stream("transactions_enr")
    df=df.filter(col("__END_AT").isNull()).groupBy(to_date(col("txn_timestamp")).alias("Transaction_Date"), col("branch_code")) \
    .agg(count("*").alias("Total_Transactions"), \
         sum(when(col("txn_status")=="SUCCESS", 1).otherwise(0)).alias("Success"), \
         sum(when(col("txn_status")=="FAILED", 1).otherwise(0)).alias("Failed"), \
         sum(when(col("txn_TYPE")=="CREDIT", col("txn_amount")).otherwise(0)).alias("Credit"), \
         sum(when(col("txn_TYPE")=="DEBIT", col("txn_amount")).otherwise(0)).alias("Debit"))
    return df



#     df.createOrReplaceTempView("aux")
#     df2=spark.sql('''SELECT to_date(txn_timestamp) AS DATE, branch_code, count(*) AS Total_Transactions, 
# SUM(CASE WHEN TXN_STATUS="SUCCESS" THEN 1 ELSE 0 END) AS SUCCESS, SUM(CASE WHEN TXN_STATUS="FAILED" THEN 1 ELSE 0 END) AS FAILED, 
# SUM(CASE WHEN TXN_TYPE="DEBIT" THEN TXN_AMOUNT ELSE 0 END) AS DEBIT, 
# SUM(CASE WHEN TXN_TYPE="CREDIT" THEN TXN_AMOUNT ELSE 0 END) AS CREDIT from aux
# WHERE __END_AT IS NULL
# GROUP BY to_date(txn_timestamp), branch_code''')
#     return df2
