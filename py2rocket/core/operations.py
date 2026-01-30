"""
DSL Operations para Stratio Rocket

Define las operaciones disponibles en el DSL:
- sql: Ejecutar queries SQL
- pyspark: Ejecutar transformaciones PySpark
- print_step: Imprimir resultados para debugging

Estas funciones son los building blocks del DSL declarativo.
"""

from typing import Optional, Union, List
from py2rocket.core.pipeline import (
    Node,
    Edge,
    StepType,
    ExecutionEngine,
    DataRelation,
    StepResult,
    Pipeline,
)


# Variable global para almacenar el pipeline en construcción
_current_pipeline: Optional[Pipeline] = None


def get_current_pipeline() -> Pipeline:
    """Obtiene el pipeline actualmente en construcción"""
    if _current_pipeline is None:
        raise RuntimeError("No hay un pipeline activo. Usa @pipeline decorator.")
    return _current_pipeline


def set_current_pipeline(pipeline: Pipeline) -> None:
    """Establece el pipeline actual"""
    global _current_pipeline
    _current_pipeline = pipeline


def sql(
    name: str,
    query: str,
    priority: int = 50,
    cache_table: bool = False,
    force_native_query: bool = False,
    description: str = "",
) -> StepResult:
    """
    Define un paso de entrada SQL.

    Ejecuta una query SQL sobre las fuentes de datos configuradas en Rocket.
    Soporta parámetros mediante sintaxis {{NOMBRE_PARAMETRO}}.

    Args:
        name: Nombre único del paso
        query: Query SQL a ejecutar. Puede incluir parámetros: {{P_TABLA}}
        priority: Prioridad de ejecución (menor número = ejecuta primero)
        cache_table: Si se debe cachear el resultado en memoria
        force_native_query: Forzar ejecución nativa de la query
        description: Descripción del propósito de este paso

    Returns:
        StepResult que puede ser usado como input en otros pasos

    Example:
        >>> tabla = sql(
        ...     name="Load_Ventas",
        ...     query="SELECT * FROM {{P_TABLA}} WHERE fecha >= '2024-01-01'",
        ...     priority=10
        ... )
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="SQLInputStep",
        class_pretty_name="SQL",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "query": query,
            "forceNativeQuery": force_native_query,
            "cacheTable": cache_table,
            "isSaved": True,
            "asyncRefresh": False,
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
        },
        supported_engines=["Batch", "Hybrid"],
    )

    pipeline.add_node(node)
    return StepResult(node, pipeline)


def pyspark(
    name: str,
    code: str,
    inputs: Optional[Union[StepResult, List[StepResult]]] = None,
    priority: int = 50,
    description: str = "",
) -> StepResult:
    """
    Define un paso de transformación PySpark.

    Ejecuta código PySpark personalizado para transformar datos.

    Args:
        name: Nombre único del paso
        code: Código PySpark a ejecutar
        inputs: Paso(s) previo(s) que alimentan esta transformación
        priority: Prioridad de ejecución
        description: Descripción de la transformación

    Returns:
        StepResult que puede ser usado en pasos posteriores

    Example:
        >>> base = sql(name="Load", query="SELECT * FROM tabla")
        >>> filtrado = pyspark(
        ...     name="Filtrar_Activos",
        ...     code="df.filter(col('estado') == 'activo')",
        ...     inputs=base
        ... )
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.TRANSFORM,
        class_name="PySparkTransformStep",
        class_pretty_name="PySpark",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={"code": code, "isSaved": True},
        supported_engines=["Batch", "Streaming", "Hybrid"],
    )

    pipeline.add_node(node)

    # Crear edges desde los inputs
    if inputs is not None:
        input_list = inputs if isinstance(inputs, list) else [inputs]
        for input_step in input_list:
            edge = Edge(
                origin=input_step.node.name,
                destination=name,
                data_type=DataRelation.VALID_DATA,
            )
            pipeline.add_edge(edge)

    return StepResult(node, pipeline)


def print_step(
    input_step: StepResult,
    priority: int = 50,
    print_data: bool = False,
    print_schema: bool = False,
    print_metadata: bool = True,
    log_level: str = "warn",
) -> StepResult:
    """
    Define un paso de salida para imprimir/mostrar datos.

    Útil para debugging y validación. Imprime información sobre el DataFrame
    sin persistirlo.

    Args:
        input_step: Paso previo del cual se imprimirán los datos
        priority: Prioridad de ejecución
        print_data: Si se deben imprimir los datos (puede ser costoso)
        print_schema: Si se debe imprimir el schema del DataFrame
        print_metadata: Si se deben imprimir metadatos (filas, columnas, etc.)
        log_level: Nivel de log (debug, info, warn, error)

    Returns:
        StepResult del paso print

    Example:
        >>> tabla = sql(name="Load", query="SELECT * FROM tabla")
        >>> print_step(tabla, print_schema=True)
    """
    pipeline = get_current_pipeline()

    # Generar nombre único para el print
    print_name = f"Print"

    node = Node(
        name=print_name,
        step_type=StepType.OUTPUT,
        class_name="PrintOutputStep",
        class_pretty_name="Print",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        configuration={
            "printData": print_data,
            "printSchema": print_schema,
            "printMetadata": print_metadata,
            "logLevel": log_level,
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
    )

    pipeline.add_node(node)

    # Crear edge desde el input
    edge = Edge(
        origin=input_step.node.name,
        destination=print_name,
        data_type=DataRelation.VALID_DATA,
    )
    pipeline.add_edge(edge)

    # Actualizar outputsWriter del nodo de entrada
    for n in pipeline.nodes:
        if n.name == input_step.node.name:
            if "outputsWriter" not in n.to_dict():
                n.to_dict()["outputsWriter"] = []
            n.to_dict()["outputsWriter"].append(
                {
                    "saveMode": "Append",
                    "outputStepName": print_name,
                    "tableName": "",
                    "discardTableName": "",
                    "extraOptions": {"checkIfEmpty": False},
                }
            )

    return StepResult(node, pipeline)
