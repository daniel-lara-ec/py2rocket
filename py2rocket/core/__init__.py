"""
py2rocket.core - Componentes core del módulo
"""

from py2rocket.core.pipeline import (
    Pipeline,
    Node,
    Edge,
    ExecutionEngine,
    StepType,
    DataRelation,
    OutputWriter,
    StepResult,
    StepResultOutput,
)

from py2rocket.core.debug_options import DebugOptions, get_default_debug_options

# Importar operaciones de los módulos específicos - Input
from py2rocket.core.input import (
    sql,
    jdbc,
    postgres,
    parquet,
    delta,
    json,
    csv,
    filesystem,
    pyspark_input,
    custom_lite_xd,
    sftp_input,
    test_input,
)

# Importar operaciones de los módulos específicos - Transformation
from py2rocket.core.transformation import (
    add_columns,
    drop_columns,
    rename_columns,
    coalesce,
    persist,
    repartition,
    bypass,
    union,
    pyspark,
    trigger,
    filter,
    custom_lite_xd_transform,
    ml_model,
)

# Importar operaciones de los módulos específicos - Output
from py2rocket.core.output import (
    jdbc_output,
    postgres_output,
    sftp_output,
    delta_output,
    parquet_output,
    json_output,
    csv_output,
    text_output,
    print_step,
    run_workflow,
    pyspark_output,
    custom_lite_xd_output,
)

from py2rocket.core.decorators import pipeline
from py2rocket.core.compiler import RocketCompiler

__all__ = [
    # Core pipeline
    "Pipeline",
    "Node",
    "Edge",
    "ExecutionEngine",
    "StepType",
    "DataRelation",
    "OutputWriter",
    "StepResult",
    # Debug options
    "DebugOptions",
    "get_default_debug_options",
    # Input operations - Database
    "sql",
    "jdbc",
    "postgres",
    # Input operations - Structured files
    "parquet",
    "delta",
    "json",
    # Input operations - Unstructured files
    "csv",
    "filesystem",
    # Input operations - Python
    "pyspark_input",
    # Input operations - CustomMade
    "custom_lite_xd",
    # Input operations - Special
    "sftp_input",
    "test_input",
    # Transformation operations - Column operations
    "add_columns",
    "drop_columns",
    "rename_columns",
    # Transformation operations - Optimization
    "coalesce",
    "persist",
    "repartition",
    # Transformation operations - Other
    "bypass",
    "filter",
    "union",
    # Transformation operations - Python
    "pyspark",
    # Transformation operations - SQL
    "trigger",
    # Transformation operations - CustomMade
    "custom_lite_xd_transform",
    # Transformation operations - MlModel
    "ml_model",
    # Output operations - Database
    "jdbc_output",
    "postgres_output",
    "sftp_output",
    # Output operations - Structured files
    "delta_output",
    "parquet_output",
    "json_output",
    # Output operations - Unstructured files
    "csv_output",
    "text_output",
    # Output operations - Other
    "print_step",
    "run_workflow",
    # Output operations - Python
    "pyspark_output",
    # Output operations - CustomMade
    "custom_lite_xd_output",
    # Decorators and compiler
    "pipeline",
    "RocketCompiler",
]
