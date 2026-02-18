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
    # Special inputs
    sftp_input,
    test_input,
    get_current_pipeline as _get_current_pipeline_input,
    set_current_pipeline as _set_current_pipeline_input,
)

from py2rocket.core.transformation import (
    # Column operations
    add_columns,
    drop_columns,
    select,
    distinct,
    drop_duplicates,
    rename_columns,
    # Optimization operations
    coalesce,
    persist,
    repartition,
    # Other operations
    bypass,
    filter,
    union,
    # Python operations
    pyspark,
    # SQL operations
    trigger,
    # CustomMade operations
    custom_lite_xd_transform,
    # MlModel operations
    ml_model,
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

from typing import Optional, Union, List, Dict, Any, Tuple
import inspect
import functools
from py2rocket.core.step_defaults import _get_step_defaults
from py2rocket.core.pipeline import (
    Pipeline,
    Node,
    Edge,
    StepType,
    ExecutionEngine,
    DataRelation,
    StepResult,
    StepResultOutput,
)


# Variable global compartida entre todos los módulos
_current_pipeline: Optional[Pipeline] = None


def get_current_pipeline() -> Pipeline:
    """Obtiene el pipeline actualmente en construcción"""
    global _current_pipeline
    if _current_pipeline is not None:
        return _current_pipeline

    for getter in (
        _get_current_pipeline_input,
        _get_current_pipeline_transform,
        _get_current_pipeline_output,
    ):
        try:
            pipe = getter()
            _current_pipeline = pipe
            return pipe
        except RuntimeError:
            continue

    raise RuntimeError("No hay un pipeline activo. Usa @pipeline decorator.")


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


def _normalize_step_type(step_type: Union[str, StepType]) -> StepType:
    """Normaliza el tipo de paso a enum StepType."""
    if isinstance(step_type, StepType):
        return step_type
    step_type_str = str(step_type).strip().lower()
    if step_type_str == "input":
        return StepType.INPUT
    if step_type_str == "output":
        return StepType.OUTPUT
    return StepType.TRANSFORMATION


def _normalize_execution_engine(
    execution_engine: Union[str, ExecutionEngine],
) -> ExecutionEngine:
    """Normaliza el motor de ejecución a enum ExecutionEngine."""
    if isinstance(execution_engine, ExecutionEngine):
        return execution_engine
    engine_str = str(execution_engine).strip().lower()
    if engine_str == "batch":
        return ExecutionEngine.BATCH
    if engine_str == "streaming":
        return ExecutionEngine.STREAMING
    return ExecutionEngine.HYBRID


def _get_origin_and_relation(input_step: Union[StepResult, StepResultOutput]) -> tuple:
    """
    Extrae el nodo de origen y su tipo de relación de datos.

    Args:
        input_step: Un StepResult o StepResultOutput

    Returns:
        Tupla (node_name, data_relation) para crear el edge correctamente
    """
    if isinstance(input_step, StepResultOutput):
        return input_step.node.name, input_step.data_relation
    return input_step.node.name, DataRelation.VALID_DATA


def raw_step(
    name: str,
    step_type: Optional[Union[str, StepType]] = None,
    class_name: Optional[str] = None,
    class_pretty_name: Optional[str] = None,
    configuration: Optional[Dict[str, Any]] = None,
    inputs: Optional[Union[StepResult, StepResultOutput, List]] = None,
    arity: Optional[List[str]] = None,
    execution_engine: Optional[Union[str, ExecutionEngine]] = None,
    priority: Optional[int] = None,
    supported_engines: Optional[List[str]] = None,
    supported_data_relations: Optional[List[str]] = None,
    description: str = "",
    outputs_writer: Optional[List[Dict[str, Any]]] = None,
    ui_configuration: Optional[Dict[str, Any]] = None,
    lineage_properties: Optional[List[Any]] = None,
    last_modified: Optional[str] = None,
    include_debug_options: Optional[bool] = None,
    include_supported_data_relations: Optional[bool] = None,
    include_description: Optional[bool] = None,
    include_node_metadata: Optional[bool] = None,
    node_metadata: Optional[Any] = None,
) -> StepResult:
    """
    Crea un nodo de forma directa a partir de su parametría completa.

    Útil para reconstrucciones exactas desde JSON sin modificar atributos
    después de la creación del nodo.
    """
    pipeline = get_current_pipeline()

    defaults = _get_step_defaults(
        class_name=class_name, step_type=step_type, class_pretty_name=class_pretty_name
    )

    if class_name is None and defaults:
        class_name = defaults.get("className")

    if step_type is None:
        step_type = defaults.get("stepType") if defaults else "Transformation"

    if class_pretty_name is None:
        class_pretty_name = defaults.get("classPrettyName") if defaults else ""

    default_config = defaults.get("configuration", {}) if defaults else {}
    merged_config = configuration or {}

    if priority is None:
        default_priority = (
            default_config.get("priority") if isinstance(default_config, dict) else None
        )
        try:
            priority = (
                int(str(default_priority)) if default_priority is not None else 50
            )
        except Exception:
            priority = 50

    if execution_engine is None:
        execution_engine = (
            defaults.get("executionEngine") if defaults else ExecutionEngine.HYBRID
        )

    if arity is None and defaults and "arity" in defaults:
        arity = defaults.get("arity")

    if supported_engines is None and defaults and "supportedEngines" in defaults:
        supported_engines = defaults.get("supportedEngines")

    if (
        supported_data_relations is None
        and defaults
        and "supportedDataRelations" in defaults
    ):
        supported_data_relations = defaults.get("supportedDataRelations")

    if outputs_writer is None and defaults and "outputsWriter" in defaults:
        outputs_writer = defaults.get("outputsWriter")

    if ui_configuration is None and defaults and "uiConfiguration" in defaults:
        ui_configuration = defaults.get("uiConfiguration")

    if lineage_properties is None and defaults and "lineageProperties" in defaults:
        lineage_properties = defaults.get("lineageProperties")

    if last_modified is None and defaults and "lastModified" in defaults:
        last_modified = defaults.get("lastModified")

    if include_debug_options is None:
        include_debug_options = "debugOptions" in merged_config

    if not include_debug_options:
        merged_config.pop("debugOptions", None)

    if include_supported_data_relations is None:
        include_supported_data_relations = True

    if include_description is None:
        include_description = True

    # NodeMetadata: determine include_node_metadata flag
    if include_node_metadata is None:
        # Check if any metadata fields exist in config
        has_metadata = any(
            k in merged_config
            for k in [
                "isSaved",
                "genAIMetadataTableDescription",
                "genAIMetadataColumns",
                "genAIMetadataTablesDescription",
            ]
        )
        include_node_metadata = has_metadata

    # Remove metadata from merged_config as it will be handled separately
    if not include_node_metadata:
        for key in [
            "isSaved",
            "genAIMetadataTableDescription",
            "genAIMetadataColumns",
            "genAIMetadataTablesDescription",
        ]:
            merged_config.pop(key, None)

    node_kwargs = {
        "name": name,
        "step_type": _normalize_step_type(step_type),
        "class_name": class_name or "",
        "class_pretty_name": class_pretty_name or "",
        "priority": priority,
        "description": description,
        "configuration": merged_config,
        "execution_engine": _normalize_execution_engine(execution_engine),
    }

    if arity is not None:
        node_kwargs["arity"] = arity
    if supported_engines is not None:
        node_kwargs["supported_engines"] = supported_engines
    if supported_data_relations is not None:
        node_kwargs["supported_data_relations"] = supported_data_relations
    if outputs_writer is not None:
        node_kwargs["outputs_writer"] = outputs_writer
    if ui_configuration is not None:
        node_kwargs["ui_configuration"] = ui_configuration
    if lineage_properties is not None:
        node_kwargs["lineage_properties"] = lineage_properties
    if last_modified is not None:
        node_kwargs["last_modified"] = last_modified
    if include_debug_options is not None:
        node_kwargs["include_debug_options"] = include_debug_options
    if include_supported_data_relations is not None:
        node_kwargs["include_supported_data_relations"] = (
            include_supported_data_relations
        )
    if include_description is not None:
        node_kwargs["include_description"] = include_description
    if include_node_metadata is not None:
        node_kwargs["include_node_metadata"] = include_node_metadata
    if node_metadata is not None:
        node_kwargs["node_metadata"] = node_metadata

    node = Node(**node_kwargs)
    pipeline.add_node(node)

    if inputs is not None:
        input_list = inputs if isinstance(inputs, list) else [inputs]
        for input_step in input_list:
            origin_name, data_relation = _get_origin_and_relation(input_step)
            edge = Edge(
                origin=origin_name,
                destination=name,
                data_type=data_relation,
            )
            pipeline.add_edge(edge)

    return StepResult(node, pipeline)


def _wrap_step(func):
    """Envuelve operaciones para soportar extra_config, ui_position y node_overrides (deprecated).

    Usa @functools.wraps para preservar la firma, type hints y metadatos de la función original,
    permitiendo autocompletado completo en IDEs.
    """
    sig = inspect.signature(func)
    valid_params = set(sig.parameters.keys())

    @functools.wraps(func)
    def wrapper(*args, **all_kwargs):
        from py2rocket.core.pipeline import UIPosition

        # Extraer parámetros específicos del wrapper
        extra_config = all_kwargs.pop("extra_config", None)
        node_overrides = all_kwargs.pop("node_overrides", None)
        config_override = all_kwargs.pop("config_override", None)
        ui_position = all_kwargs.pop("ui_position", None)

        # Los restantes van a la función original
        filtered_kwargs = {k: v for k, v in all_kwargs.items() if k in valid_params}
        result = func(*args, **filtered_kwargs)

        # Update configuration with overrides (merge, don't replace)
        if config_override is not None:
            result.node.configuration.update(config_override)
        elif extra_config:
            result.node.configuration.update(extra_config)

        # Handle UI position (new clean way)
        if ui_position is not None:
            if isinstance(ui_position, UIPosition):
                result.node.ui_configuration = ui_position.to_dict()
            elif isinstance(ui_position, dict):
                result.node.ui_configuration = ui_position

        # Handle node_overrides (deprecated but kept for backwards compatibility)
        if node_overrides:
            for key, value in node_overrides.items():
                setattr(result.node, key, value)

        return result

    return wrapper


# Wrappers comentados - Las operaciones se exportan sin wrapping
# La funcionalidad de extra_config, ui_position y node_overrides no es soportada
# Se recomienda:
# - Importar directamente desde los módulos específicos si necesitas esas características
# - O pasar configuración a nivel del pipeline en lugar de a nivel de función

# sql = _wrap_step(sql)
# jdbc = _wrap_step(jdbc)
# postgres = _wrap_step(postgres)
# parquet = _wrap_step(parquet)
# delta = _wrap_step(delta)
# json = _wrap_step(json)
# csv = _wrap_step(csv)
# filesystem = _wrap_step(filesystem)
# pyspark_input = _wrap_step(pyspark_input)
# custom_lite_xd = _wrap_step(custom_lite_xd)
# sftp_input = _wrap_step(sftp_input)
# test_input = _wrap_step(test_input)

# add_columns = _wrap_step(add_columns)
# drop_columns = _wrap_step(drop_columns)
# rename_columns = _wrap_step(rename_columns)
# coalesce = _wrap_step(coalesce)
# persist = _wrap_step(persist)
# repartition = _wrap_step(repartition)
# bypass = _wrap_step(bypass)
# filter = _wrap_step(filter)
# union = _wrap_step(union)
# pyspark = _wrap_step(pyspark)
# trigger = _wrap_step(trigger)
# custom_lite_xd_transform = _wrap_step(custom_lite_xd_transform)
# ml_model = _wrap_step(ml_model)

# jdbc_output = _wrap_step(jdbc_output)
# postgres_output = _wrap_step(postgres_output)
# sftp_output = _wrap_step(sftp_output)
# delta_output = _wrap_step(delta_output)
# parquet_output = _wrap_step(parquet_output)
# json_output = _wrap_step(json_output)
# csv_output = _wrap_step(csv_output)
# text_output = _wrap_step(text_output)
# print_step = _wrap_step(print_step)
# run_workflow = _wrap_step(run_workflow)
# pyspark_output = _wrap_step(pyspark_output)
# custom_lite_xd_output = _wrap_step(custom_lite_xd_output)


__all__ = [
    # Funciones de gestión de pipeline
    "get_current_pipeline",
    "set_current_pipeline",
    "raw_step",
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
    "select",
    "distinct",
    "drop_duplicates",
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
]
