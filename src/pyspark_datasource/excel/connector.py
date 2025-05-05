"""
Excel data source connector for PySpark using the new Python Data Source API.
"""

from typing import Any, Dict, Iterator, Optional, Tuple, List, Set
import os

import pandas as pd
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition  # type: ignore
from pyspark.sql.types import (  # type: ignore
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
)
from pyspark.sql.utils import AnalysisException  # type: ignore


def infer_spark_type(dtype: str) -> Any:
    """
    Infer Spark SQL type from pandas dtype.

    Args:
        dtype: Pandas dtype string.

    Returns:
        Corresponding Spark SQL type.
    """
    if "int" in str(dtype):
        return IntegerType()
    elif "float" in str(dtype):
        return DoubleType()
    elif "bool" in str(dtype):
        return BooleanType()
    elif "datetime" in str(dtype):
        return TimestampType()
    elif "date" in str(dtype):
        return DateType()
    else:
        return StringType()


class ExcelInputPartition(InputPartition):
    def __init__(self, file_path: str):
        self.file_path = file_path


class ExcelDataSource(DataSource):
    """Excel data source for PySpark using the new Python Data Source API."""

    @classmethod
    def name(cls) -> str:
        """Return the name of the data source."""
        return "excel"

    def schema(self) -> StructType:
        """
        Determine the schema for the Excel file, matching Spark's standard behavior:
        - If a schema is supplied, use it exactly.
        - If no schema and inferSchema=false, all columns are StringType.
        - If no schema and inferSchema=true, infer types from the file.
        """
        user_schema = getattr(self, "_user_schema", None)
        if user_schema is not None:
            return user_schema

        infer_schema = self.options.get("inferSchema", "true").lower() == "true"
        path = self.options.get("path")
        if not path:
            raise ValueError("'path' option is required to infer schema from the file")
        sheet_name = self.options.get("sheetName", 0)
        header = self.options.get("header", "true").lower() == "true"
        try:
            df = pd.read_excel(
                path,
                sheet_name=sheet_name,
                header=0 if header else None,
                nrows=0,
                dtype_backend="pyarrow",
            )
            if not infer_schema:
                # All columns as StringType
                fields = [StructField(str(col_name), StringType(), True) for col_name in df.columns]
                return StructType(fields)
            else:
                # Infer types
                fields = [StructField(str(col_name), infer_spark_type(dtype), True) for col_name, dtype in df.dtypes.items()]
                return StructType(fields)
        except Exception as e:
            raise AnalysisException(f"Failed to determine schema from Excel file: {str(e)}")

    def reader(self, schema: StructType) -> "ExcelDataSourceReader":
        """
        Create a reader for the Excel data source.
        If a schema is supplied, store it for use in schema().
        """
        if schema is not None:
            self._user_schema = schema
        return ExcelDataSourceReader(schema, self.options)

    def writer(self, schema: StructType, overwrite: bool) -> "ExcelDataSourceWriter":
        """
        Create a writer for the Excel data source.

        Args:
            schema: The schema to use for writing the data.
            overwrite: Whether to overwrite existing data.

        Returns:
            An instance of ExcelDataSourceWriter.
        """
        return ExcelDataSourceWriter(schema, self.options, overwrite)

    def streamReader(self, schema: StructType) -> "ExcelDataSourceStreamReader":
        return ExcelDataSourceStreamReader(schema, self.options)


class ExcelDataSourceReader(DataSourceReader):
    """Reader for Excel data source."""

    def __init__(self, schema: StructType, options: Dict[str, str]):
        """
        Initialize the Excel data source reader.

        Args:
            schema: The schema to use for reading the data.
            options: Options for reading the Excel file.
        """
        self.schema = schema
        self.options = options

    def partitions(self) -> List[InputPartition]:
        """
        Return the partitions for reading the Excel file(s).
        Each partition corresponds to a file. If a directory is provided, each .xlsx file is a partition.

        Returns:
            A list of InputPartition objects, each initialized with a file path.
        """
        path = self.options.get("path")
        if not path:
            raise ValueError("'path' option is required")

        if os.path.isdir(path):
            files = [
                os.path.join(path, f)
                for f in os.listdir(path)
                if f.lower().endswith(".xlsx")
            ]
            if not files:
                raise ValueError(f"No .xlsx files found in directory: {path}")
            return [ExcelInputPartition(file) for file in files]
        elif os.path.isfile(path):
            return [ExcelInputPartition(path)]
        else:
            raise ValueError(f"Path '{path}' is not a valid file or directory")

    def read(self, partition: ExcelInputPartition) -> Iterator[Tuple]:
        """
        Read data from the Excel file.

        Args:
            partition: The partition to read from (file path).

        Yields:
            Tuples containing the data from the Excel file.

        Raises:
            AnalysisException: If the file cannot be read or is invalid.
        """
        try:
            if not isinstance(partition, ExcelInputPartition):
                raise ValueError(
                    "Partition must be an ExcelInputPartition with a file_path attribute"
                )
            file_path = partition.file_path
            sheet_name = self.options.get("sheetName", 0)
            header = self.options.get("header", "true").lower() == "true"
            enable_arrow = self.options.get("enableArrow", "false").lower() == "true"

            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                header=0 if header else None,
                dtype_backend="pyarrow",
            )

            if enable_arrow:
                import pyarrow as pa

                table = pa.Table.from_pandas(df)
                for batch in table.to_batches():
                    yield batch
            else:
                for _, row in df.iterrows():
                    yield tuple(row)

        except Exception as e:
            raise AnalysisException(f"Failed to read Excel file: {str(e)}")


class ExcelDataSourceStreamReader(DataSourceReader):
    """Streaming reader for Excel data source."""

    def __init__(self, schema: StructType, options: Dict[str, str]):
        self.schema = schema
        self.options = options
        self.seen_files: Set[str] = set()
        self.current_offset = 0

    def initialOffset(self) -> dict:
        return {"offset": 0}

    def latestOffset(self) -> dict:
        path = self.options.get("path")
        if not path or not os.path.isdir(path):
            raise ValueError("'path' must be a directory for streaming Excel source")
        files = [
            os.path.join(path, f)
            for f in os.listdir(path)
            if f.lower().endswith(".xlsx")
        ]
        new_files = [f for f in files if f not in self.seen_files]
        self.current_offset += len(new_files)
        return {"offset": self.current_offset}

    def partitions(self, start: dict, end: dict):
        path = self.options.get("path")
        if not path or not os.path.isdir(path):
            raise ValueError("'path' must be a directory for streaming Excel source")
        files = [
            os.path.join(path, f)
            for f in os.listdir(path)
            if f.lower().endswith(".xlsx")
        ]
        new_files = [f for f in files if f not in self.seen_files]
        for f in new_files:
            self.seen_files.add(f)
        return [ExcelInputPartition(f) for f in new_files]

    def commit(self, end: dict):
        pass  # No-op for now

    def read(self, partition: ExcelInputPartition) -> Iterator[Tuple]:
        try:
            if not isinstance(partition, ExcelInputPartition):
                raise ValueError(
                    "Partition must be an ExcelInputPartition with a file_path attribute"
                )
            file_path = partition.file_path
            sheet_name = self.options.get("sheetName", 0)
            header = self.options.get("header", "true").lower() == "true"
            enable_arrow = self.options.get("enableArrow", "false").lower() == "true"

            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                header=0 if header else None,
                dtype_backend="pyarrow",
            )

            if enable_arrow:
                import pyarrow as pa

                table = pa.Table.from_pandas(df)
                for batch in table.to_batches():
                    yield batch
            else:
                for _, row in df.iterrows():
                    yield tuple(row)

        except Exception as e:
            raise AnalysisException(f"Failed to read Excel file (streaming): {str(e)}")


class ExcelDataSourceWriter:
    """Writer for Excel data source."""

    def __init__(self, schema: StructType, options: Dict[str, str], overwrite: bool):
        """
        Initialize the Excel data source writer.

        Args:
            schema: The schema to use for writing the data.
            options: Options for writing the Excel file.
            overwrite: Whether to overwrite existing data.
        """
        self.schema = schema
        self.options = options
        self.overwrite = overwrite

    def write(self, iterator: Iterator[Tuple]) -> None:
        """
        Write data to the Excel file.

        Args:
            iterator: Iterator of tuples containing the data to write.

        Raises:
            AnalysisException: If the data cannot be written.
        """
        try:
            path = self.options.get("path")
            if not path:
                raise ValueError("'path' option is required")

            sheet_name = self.options.get("sheetName", "Sheet1")

            # Convert iterator to pandas DataFrame
            df = pd.DataFrame(
                iterator, columns=[field.name for field in self.schema.fields]
            )

            # Write to Excel file
            df.to_excel(path, sheet_name=sheet_name, index=False, engine="openpyxl")

        except Exception as e:
            raise AnalysisException(f"Failed to write Excel file: {str(e)}")
