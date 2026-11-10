# Dead Ends & Failed Approaches Log

| Iteration | Approach Tried | Why It Failed | Files Touched |
|-----------|---------------|---------------|---------------|
| M2 Iteration 1 | Writing partitioned Parquet using Spark `.mode("overwrite")` without pre-purging destination directory | Overwrite mode does not delete directories with different partition string encoding (Windows literal `province_name=Gia Lai` vs Linux Docker URL-encoded `province_name=Gia%20Lai`). Resulted in 15 partition directories in `lakehouse_storage/silver/tedp_station/` instead of 11, causing `test_silver_station_dataset_integrity` to fail and doubling row count on PyArrow read. | `code/lakehouse/spark_preprocess_tedp.py`, `code/lakehouse/spark_sync_province.py` |

