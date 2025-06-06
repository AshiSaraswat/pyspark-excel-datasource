# PySpark Data Source Connectors

This project provides a custom data source connector for PySpark, supporting Excel files.

## Features

- Excel file connector for PySpark with automatic schema inference
- Modern Python project structure
- Type hints and comprehensive testing

## Prerequisites

- [UV](https://github.com/astral-sh/uv) for Python, virtual environment, and dependency management

## Installation

```bash
pip install pyspark-datasource
```

## Development Setup

1. **Install UV** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```
   Or see [UV installation instructions](https://docs.astral.sh/uv/getting-started/installation/).

2. **Install the desired Python version (e.g., 3.12) using UV:**
   ```bash
   uv python install 3.12
   ```
   This will download and install Python 3.12 if it is not already available on your system.

3. **Create a virtual environment with UV (using Python 3.12):**
   ```bash
   uv venv --python=3.12
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

4. **Install project dependencies:**
   ```bash
   uv sync
   ```
   This will install all required dependencies as specified in your `pyproject.toml`.

5. **Install development dependencies:**
   ```bash
   uv sync --all-groups
   ```
   This will install all main and development dependencies (e.g., pytest, ruff, mypy).

6. **Add new dependencies:**
   - To add a regular dependency:
     ```bash
     uv add <package-name>
     ```
   - To add a dev dependency:
     ```bash
     uv add --dev <package-name>
     ```
   This will update your `pyproject.toml` and install the package immediately.

For more details, see the [UV Managing dependencies documentation](https://docs.astral.sh/uv/concepts/projects/dependencies/) and [UV Sync documentation](https://docs.astral.sh/uv/concepts/projects/sync/).

## Development Tools

This project uses:
- [Ruff](https://github.com/astral-sh/ruff) for linting and formatting
- [mypy](https://mypy.readthedocs.io/) for type checking
- [pytest](https://docs.pytest.org/) for testing

Common development commands:
```bash
# Run tests
uv run test

# Run linting
uv run lint

# Run formatting
uv run format

# Run type checking
uv run typecheck
```

## Project Structure

```
pyspark-datasource/
├── src/
│   └── pyspark_datasource/
│       ├── __init__.py
│       └── excel/
│           ├── __init__.py
│           └── connector.py
├── tests/
│   ├── __init__.py
│   └── test_excel_connector.py
├── pyproject.toml
├── LICENSE
└── README.md
```

## Usage

### Excel Connector

The Excel connector supports automatic schema inference and provides a simple interface for reading and writing Excel files.

```python
from pyspark.sql import SparkSession
from pyspark_datasource.excel import ExcelDataSource

spark = SparkSession.builder.getOrCreate()

# Register the data source
spark.dataSource.register(ExcelDataSource)

# Read Excel file with automatic schema inference
df = spark.read.format("excel") \
    .option("path", "path/to/file.xlsx") \
    .option("sheetName", "Sheet1") \
    .option("header", "true") \
    .load()

# Write DataFrame to Excel file
df.write.format("excel") \
    .option("path", "path/to/output.xlsx") \
    .option("sheetName", "Sheet1") \
    .save()
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
- Follow PEP 8 style guidelines
- Write comprehensive tests for new features
- Update documentation for API changes
- Use type hints throughout the codebase

## Authors and Maintainers

### Authors
- [Ashish Saraswat](https://github.com/AshiSaraswat)
- [Sourav Gulati](https://github.com/sgulati89)

### Maintainers
- [Ashish Saraswat](https://github.com/AshiSaraswat)
- [Sourav Gulati](https://github.com/sgulati89)

### Acknowledgments



## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Citation

If you use this project in your research or work, please cite:

```bibtex
@software{pyspark_excel_datasource,
  author = {Saraswat, Ashish and Gulati, Sourav},
  title = {PySpark Excel Data Source Connector},
  url = {https://github.com/AshiSaraswat/pyspark-excel-datasource},
  version = {0.1.0},
  year = {2024}
}
```