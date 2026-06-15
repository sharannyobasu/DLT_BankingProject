import dlt
from pyspark.sql.functions import *
expect_rules={
    "rule1" : "txn_id IS NOT NULL",
    "rule2" : "currency = 'INR'"
}

expect_or_drop_rules={
    "rule3" : "txn_amount >0",
    "rule4" : "txn_type in ('DEBIT', 'CREDIT')",
    "rule5" : "txn_status in ('SUCCESS', 'FAILED')",
    "rule6" : "customer_id is NOT NULL"
}

@dlt.view(
    name="transactions_refined"
)

@dlt.expect_all(expect_rules)
@dlt.expect_all_or_drop(expect_or_drop_rules)
def transactions_refined():
    df=spark.readStream.table("transactions")
    return df

dlt.create_streaming_table(
    name="transactions_enr"
)

dlt.create_auto_cdc_flow(
    target="transactions_enr",
    source="transactions_refined",
    sequence_by="txn_timestamp",
    keys=["txn_id"],
    stored_as_scd_type=2
)
