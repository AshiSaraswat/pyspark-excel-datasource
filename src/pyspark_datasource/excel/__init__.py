"""
Excel connector for PySpark using the new Python Data Source API.

This module provides functionality to read and write Excel files using PySpark 4.0's
new Python Data Source API.
"""

from .connector import ExcelDataSource

__all__ = ["ExcelDataSource"]
