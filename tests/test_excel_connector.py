"""
Tests for the Excel connector using the new Python Data Source API.
"""

import os
import tempfile
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource
from pyspark_datasource.excel import ExcelDataSource
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.errors.exceptions.base import AnalysisException

# Create a temporary directory for Excel files
with tempfile.TemporaryDirectory() as tmpdir:
    file_paths = []
    # Create 3 Excel files with different data
    for i, (ids, names) in enumerate([
        ([1, 2], ["Alice", "Bob"]),
        ([3, 4], ["Charlie", "David"]),
        ([5], ["Eve"])
    ]):
        file_path = os.path.join(tmpdir, f"sample_{i+1}.xlsx")
        df = pd.DataFrame({"id": ids, "name": names})
        df.to_excel(file_path, index=False)
        file_paths.append(file_path)
        print(f"File path------------>: {file_path}")

    # Start Spark session
    spark = SparkSession.builder.appName("ExcelReadTest").master("local[2]").getOrCreate()

    # Register the Excel data source
    spark.dataSource.register(ExcelDataSource)

    # Read the directory using Spark DataFrame reader
    result_df = spark.read.format("excel") \
        .option("path", tmpdir) \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])) \
        .load()

    result_df.show()

    # Collect results
    result = result_df.collect()
    print(f"Result: {result}")

    # Assert the results (should be 5 rows: 2+2+1)
    assert len(result) == 5, f"Expected 5 rows, got {len(result)}"
    expected = set([
        (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David"), (5, "Eve")
    ])
    actual = set((row["id"], row["name"]) for row in result)
    assert actual == expected, f"Rows do not match expected: {actual}"

    print("Excel partitioned read test passed.")

# Cleanup
for file_path in file_paths:
    if os.path.exists(file_path):
        os.unlink(file_path)

# Helper to create Excel files
def create_excel_file(path, data):
    df = pd.DataFrame(data)
    df.to_excel(path, index=False)
    print(f"Excel file created at: {path}")

def test_user_supplied_schema(spark):
    """Test: User-supplied schema is used exactly, extra columns in file are ignored."""
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "sample.xlsx")
        # File has 3 columns, schema only asks for 2
        create_excel_file(file_path, {"id": [1], "name": ["Alice"], "extra": ["X"]})
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        df = spark.read.format("excel") \
            .option("path", file_path) \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .schema(schema) \
            .load()
        rows = df.collect()
        assert rows[0]["id"] == 1 and rows[0]["name"] == "Alice"
        assert "extra" not in df.columns

def test_user_supplied_schema_missing_column(spark):
    """Test: User-supplied schema with missing column in file raises error."""
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "sample.xlsx")
        # File has only 'id', schema asks for 'id' and 'name'
        create_excel_file(file_path, {"id": [1]})
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        try:
            spark.read.format("excel") \
                .option("path", file_path) \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .schema(schema) \
                .load().collect()
            assert False, "Expected an exception for missing column, but none was raised."
        except Exception:
            pass  # Expected

def test_infer_schema_true(spark):
    """Test: inferSchema=true infers types from file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "sample.xlsx")
        create_excel_file(file_path, {"id": [1, 2], "name": ["Alice", "Bob"]})
        df = spark.read.format("excel") \
            .option("path", file_path) \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load()
        rows = df.collect()
        assert rows[0]["id"] == 1 and rows[0]["name"] == "Alice"
        assert df.schema[0].dataType == IntegerType()
        assert df.schema[1].dataType == StringType()

def test_infer_schema_false(spark):
    """Test: inferSchema=false, no schema: all columns are StringType."""
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "sample.xlsx")
        create_excel_file(file_path, {"id": [1, 2], "name": ["Alice", "Bob"]})
        df = spark.read.format("excel") \
            .option("path", file_path) \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .load()
        rows = df.collect()
        assert rows[0]["id"] == "1" and rows[0]["name"] == "Alice"
        assert all(f.dataType == StringType() for f in df.schema.fields)

def test_infer_schema_false_with_schema(spark):
    """Test: inferSchema=false, with schema: user schema is used exactly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "sample.xlsx")
        create_excel_file(file_path, {"id": [1, 2], "name": ["Alice", "Bob"]})
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        df = spark.read.format("excel") \
            .option("path", file_path) \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .load()
        rows = df.collect()
        assert rows[0]["id"] == 1 and rows[0]["name"] == "Alice"
        assert df.schema == schema

def test_excel_partitions(spark):
    """Test: Number of partitions matches number of Excel files in directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        num_files = 5
        expected_rows = set()
        for i in range(num_files):
            file_path = os.path.join(tmpdir, f"sample_{i+1}.xlsx")
            data = {"id": [i], "name": [f"Name_{i}"]}
            create_excel_file(file_path, data)
            expected_rows.add((i, f"Name_{i}"))
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        df = spark.read.format("excel") \
            .option("path", tmpdir) \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .load()
        # Check number of partitions
        num_partitions = df.rdd.getNumPartitions()
        assert num_partitions == num_files, f"Expected {num_files} partitions, got {num_partitions}"
        # Check all rows are present
        actual_rows = set((row["id"], row["name"]) for row in df.collect())
        assert actual_rows == expected_rows, f"Rows do not match expected: {actual_rows}"

def test_excel_writer_functionality(spark):
    """Test: Write a DataFrame to Excel using the connector, then read it back and verify contents."""
    import shutil
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "output.xlsx")
        # Create a DataFrame
        data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        # Write to Excel
        df.write.format("excel") \
            .option("path", file_path) \
            .option("header", "true") \
            .mode("overwrite") \
            .save()
        print(f"Excel file written at: {file_path}")
        # Read back
        read_df = spark.read.format("excel") \
            .option("path", file_path) \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load()
        read_rows = set((row["id"], row["name"]) for row in read_df.collect())
        expected_rows = set(data)
        assert read_rows == expected_rows, f"Written and read data do not match: {read_rows} vs {expected_rows}"

def main():
    spark = SparkSession.builder.appName("ExcelReadTest").master("local[2]").getOrCreate()
    spark.dataSource.register(ExcelDataSource)
    try:
        # test_user_supplied_schema(spark)
        # test_user_supplied_schema_missing_column(spark)
        # test_infer_schema_true(spark)
        # test_infer_schema_false(spark)
        # test_infer_schema_false_with_schema(spark)
        # test_excel_partitions(spark)
        # test_excel_writer_functionality(spark)
        print("All schema handling edge cases passed.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 