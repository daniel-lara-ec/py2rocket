"""
DSL Operations para Stratio Rocket

NOTA: Este módulo mantiene compatibilidad hacia atrás.
Las operaciones ahora están organizadas en módulos específicos:
- input.py: Operaciones de entrada (sql, csv, postgres, jdbc, delta, parquet, json, filesystem, pyspark_input, custom_lite_xd)
- transformation.py: Operaciones de transformación (pyspark, repartition, add_columns, drop_columns, rename_columns,
                                                     coalesce, persist, bypass, trigger, custom_lite_xd_transform)
- output.py: Operaciones de salida (print_step, run_workflow, csv_output, delta_output, parquet_output, json_output,
                                    text_output, jdbc_output, postgres_output, sftp_output, pyspark_output, custom_lite_xd_output)

Se recomienda importar directamente desde los módulos específicos para mejor organización.
"""

# Importar desde los módulos específicos para mantener compatibilidad
from py2rocket.core.input import (
    # Database inputs
    sql,
    jdbc,
    postgres,
    # Structured file inputs
    parquet,
    delta,
    json,
    # Unstructured file inputs
    csv,
    filesystem,
    # Python inputs
    pyspark_input,
    # CustomMade inputs
    custom_lite_xd,
    get_current_pipeline as _get_current_pipeline_input,
    set_current_pipeline as _set_current_pipeline_input,
)

from py2rocket.core.transformation import (
    # Column operations
    add_columns,
    drop_columns,
    rename_columns,
    # Optimization operations
    coalesce,
    persist,
    repartition,
    # Other operations
    bypass,
    # Python operations
    pyspark,
    # SQL operations
    trigger,
    # CustomMade operations
    custom_lite_xd_transform,
    get_current_pipeline as _get_current_pipeline_transform,
    set_current_pipeline as _set_current_pipeline_transform,
)

from py2rocket.core.output import (
    # Database outputs
    jdbc_output,
    postgres_output,
    sftp_output,
    # Structured file outputs
    delta_output,
    parquet_output,
    json_output,
    # Unstructured file outputs
    csv_output,
    text_output,
    # Other outputs
    print_step,
    run_workflow,
    # Python outputs
    pyspark_output,
    # CustomMade outputs
    custom_lite_xd_output,
    get_current_pipeline as _get_current_pipeline_output,
    set_current_pipeline as _set_current_pipeline_output,
)

from py2rocket.core.pipeline import Pipeline
from typing import Optional


# Variable global compartida entre todos los módulos
_current_pipeline: Optional[Pipeline] = None


def get_current_pipeline() -> Pipeline:
    """Obtiene el pipeline actualmente en construcción"""
    if _current_pipeline is None:
        raise RuntimeError("No hay un pipeline activo. Usa @pipeline decorator.")
    return _current_pipeline


def set_current_pipeline(pipeline: Pipeline) -> None:
    """
    Establece el pipeline actual y lo sincroniza con todos los módulos
    """
    global _current_pipeline
    _current_pipeline = pipeline

    # Sincronizar con todos los módulos
    _set_current_pipeline_input(pipeline)
    _set_current_pipeline_transform(pipeline)
    _set_current_pipeline_output(pipeline)


__all__ = [
    # Funciones de gestión de pipeline
    "get_current_pipeline",
    "set_current_pipeline",
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
    # Transformation operations - Python
    "pyspark",
    # Transformation operations - SQL
    "trigger",
    # Transformation operations - CustomMade
    "custom_lite_xd_transform",
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
]
