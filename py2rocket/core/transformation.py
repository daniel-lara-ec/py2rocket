"""
DSL Transformation Operations para Stratio Rocket

Define las operaciones de transformación disponibles en el DSL.
Están organizadas por categoría:

- ColumnOperation: AddColumns, DropColumns, RenameColumns
- CustomMade: CustomLiteXD
- Optimization: Coalesce, Persist, Repartition
- Other: Bypass
- Python: PySpark
- SQL: Trigger

Estas funciones transforman los datos entre pasos del pipeline.
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
    global _current_pipeline
    if _current_pipeline is None:
        raise RuntimeError("No hay un pipeline activo. Usa @pipeline decorator.")
    return _current_pipeline


def set_current_pipeline(pipeline: Pipeline) -> None:
    """Establece el pipeline actual"""
    global _current_pipeline
    _current_pipeline = pipeline


# ============================================================================
# COLUMN OPERATION TRANSFORMATIONS
# ============================================================================


def add_columns(
    name: str,
    inputs: Optional[Union[StepResult, List[StepResult]]] = None,
    select_type: str = "EXPRESSION",
    add_column_expression_list: str = "",
    columns: Optional[list] = None,
    priority: int = 50,
    description: str = "",
) -> StepResult:
    """
    Define un paso de transformación para agregar columnas.

    Añade nuevas columnas calculadas al DataFrame usando expresiones o queries SQL.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) que alimentan esta transformación
        select_type: Tipo de selección ("EXPRESSION" o "SUBQUERY")
        add_column_expression_list: Expresiones de columnas a agregar
        columns: Lista de columnas a agregar con su configuración
        priority: Prioridad de ejecución
        description: Descripción de la transformación

    Returns:
        StepResult que puede ser usado en pasos posteriores

    Example:
        >>> base = sql(name="Load", query="SELECT * FROM tabla")
        >>> con_calc = add_columns(
        ...     name="Add_Totals",
        ...     inputs=base,
        ...     add_column_expression_list="precio * cantidad AS total"
        ... )
    """
    pipeline = get_current_pipeline()

    if columns is None:
        columns = [{"field": None, "query": None, "type": "string"}]

    node = Node(
        name=name,
        step_type=StepType.TRANSFORMATION,
        class_name="AddColumnsTransformStep",
        class_pretty_name="AddColumns",
        arity=["UnaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "selectType": select_type,
            "addColumnExpressionList": add_column_expression_list,
            "columns": columns,
            "inputSchemas": "",
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
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


def drop_columns(
    name: str,
    inputs: Optional[Union[StepResult, List[StepResult]]] = None,
    columns_to_drop: Optional[list] = None,
    priority: int = 50,
    description: str = "",
) -> StepResult:
    """
    Define un paso de transformación para eliminar columnas.

    Elimina columnas específicas del DataFrame.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) que alimentan esta transformación
        columns_to_drop: Lista de nombres de columnas a eliminar
        priority: Prioridad de ejecución
        description: Descripción de la transformación

    Returns:
        StepResult que puede ser usado en pasos posteriores

    Example:
        >>> base = sql(name="Load", query="SELECT * FROM tabla")
        >>> limpio = drop_columns(
        ...     name="Remove_Temp",
        ...     inputs=base,
        ...     columns_to_drop=["temp_col", "debug_field"]
        ... )
    """
    pipeline = get_current_pipeline()

    if columns_to_drop is None:
        columns_to_drop = []

    schema_fields = [{"name": col} for col in columns_to_drop]

    node = Node(
        name=name,
        step_type=StepType.TRANSFORMATION,
        class_name="DropColumnsTransformStep",
        class_pretty_name="DropColumns",
        arity=["UnaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "schema.fields": schema_fields,
            "inputSchemas": "",
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
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


def rename_columns(
    name: str,
    inputs: Optional[Union[StepResult, List[StepResult]]] = None,
    column_mappings: Optional[dict] = None,
    priority: int = 50,
    description: str = "",
) -> StepResult:
    """
    Define un paso de transformación para renombrar columnas.

    Renombra una o más columnas del DataFrame.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) que alimentan esta transformación
        column_mappings: Diccionario {nombre_actual: nombre_nuevo}
        priority: Prioridad de ejecución
        description: Descripción de la transformación

    Returns:
        StepResult que puede ser usado en pasos posteriores

    Example:
        >>> base = sql(name="Load", query="SELECT * FROM tabla")
        >>> renombrado = rename_columns(
        ...     name="Standardize_Names",
        ...     inputs=base,
        ...     column_mappings={"old_name": "new_name", "id": "entity_id"}
        ... )
    """
    pipeline = get_current_pipeline()

    if column_mappings is None:
        column_mappings = {}

    columns = [{"name": old, "alias": new} for old, new in column_mappings.items()]

    node = Node(
        name=name,
        step_type=StepType.TRANSFORMATION,
        class_name="RenameColumnTransformationStep",
        class_pretty_name="RenameColumns",
        arity=["UnaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "columns": columns,
            "inputSchemas": "",
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
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


# ============================================================================
# CUSTOMMADE TRANSFORMATIONS
# ============================================================================


def custom_lite_xd_transform(
    name: str,
    inputs: Optional[Union[StepResult, List[StepResult]]] = None,
    custom_lite_class_type: str = "",
    priority: int = 50,
    vault_db_name: str = "",
    input_options: str = "",
    user_pass_enabled: bool = False,
    tls_enabled: bool = False,
    vault_custom_property_enabled: bool = False,
    description: str = "",
) -> StepResult:
    """
    Define una transformación CustomLiteXD personalizada.

    Permite usar extensiones personalizadas para transformaciones específicas
    o legadas.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) que alimentan esta transformación
        custom_lite_class_type: Tipo de clase CustomLite a usar
        priority: Prioridad de ejecución
        vault_db_name: Nombre de la BD del vault
        input_options: Opciones de entrada adicionales
        user_pass_enabled: Si se debe habilitar autenticación usuario/contraseña
        tls_enabled: Si se debe habilitar TLS
        vault_custom_property_enabled: Si se deben usar propiedades del vault
        description: Descripción de la transformación

    Returns:
        StepResult que puede ser usado en pasos posteriores
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.TRANSFORMATION,
        class_name="CustomLiteXDTransformStep",
        class_pretty_name="CustomLiteXD",
        arity=[],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "customLiteClassType": custom_lite_class_type,
            "vaultDbName": vault_db_name,
            "inputOptions": input_options,
            "userPassEnabled": user_pass_enabled,
            "tlsEnabled": tls_enabled,
            "vaultCustomPropertyEnabled": vault_custom_property_enabled,
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Batch", "Hybrid"],
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


# ============================================================================
# OPTIMIZATION TRANSFORMATIONS
# ============================================================================


def coalesce(
    name: str,
    inputs: Optional[Union[StepResult, List[StepResult]]] = None,
    partitions: str = "1",
    priority: int = 50,
    description: str = "",
) -> StepResult:
    """
    Define una transformación de coalescencia de particiones.

    Reduce el número de particiones sin hacer un shuffle completo de datos.
    Útil para combinar particiones antes de escribir a archivo.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) que alimentan esta transformación
        partitions: Número final de particiones deseadas
        priority: Prioridad de ejecución
        description: Descripción de la transformación

    Returns:
        StepResult que puede ser usado en pasos posteriores

    Example:
        >>> base = sql(name="Load", query="SELECT * FROM tabla")
        >>> coalescido = coalesce(
        ...     name="Coalesce_Data",
        ...     inputs=base,
        ...     partitions="4"
        ... )
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.TRANSFORMATION,
        class_name="CoalesceTransformStep",
        class_pretty_name="Coalesce",
        arity=[],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "partitions": partitions,
            "inputSchemas": "",
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
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


def persist(
    name: str,
    inputs: Optional[Union[StepResult, List[StepResult]]] = None,
    storage_level: str = "",
    priority: int = 50,
    description: str = "",
) -> StepResult:
    """
    Define un paso de persistencia de datos en caché.

    Cachea el DataFrame en memoria para optimizar cálculos posteriores.
    Útil cuando el mismo DataFrame se usa en múltiples operaciones.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) que alimentan esta transformación
        storage_level: Nivel de almacenamiento ("MEMORY", "DISK", etc.)
        priority: Prioridad de ejecución
        description: Descripción de la transformación

    Returns:
        StepResult que puede ser usado en pasos posteriores

    Example:
        >>> base = sql(name="Load", query="SELECT * FROM tabla")
        >>> cacheado = persist(
        ...     name="Cache_Data",
        ...     inputs=base,
        ...     storage_level="MEMORY"
        ... )
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.TRANSFORMATION,
        class_name="PersistTransformStep",
        class_pretty_name="Persist",
        arity=["UnaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "storageLevel": storage_level,
            "inputSchemas": "",
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
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


def repartition(
    name: str,
    inputs: Optional[Union[StepResult, List[StepResult]]] = None,
    partitions: str = "",
    columns: str = "",
    priority: int = 50,
    description: str = "",
) -> StepResult:
    """
    Define un paso de reparticionamiento.

    Reparticiona el DataFrame para optimizar la distribución de datos.
    Útil para balancear la carga entre particiones o reducir el número de archivos de salida.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) que alimentan esta transformación
        partitions: Número de particiones deseadas (vacío para auto)
        columns: Columnas por las cuales particionar (vacío para round-robin)
        priority: Prioridad de ejecución
        description: Descripción de la transformación

    Returns:
        StepResult que puede ser usado en pasos posteriores

    Example:
        >>> base = sql(name="Load", query="SELECT * FROM tabla")
        >>> particionado = repartition(
        ...     name="Repartition_Data",
        ...     inputs=base,
        ...     partitions="10",
        ...     columns="fecha"
        ... )
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.TRANSFORMATION,
        class_name="RepartitionTransformStep",
        class_pretty_name="Repartition",
        arity=[],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "partitions": partitions,
            "columns": columns,
            "inputSchemas": "",
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
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


# ============================================================================
# OTHER TRANSFORMATIONS
# ============================================================================


def bypass(
    name: str,
    inputs: Optional[Union[StepResult, List[StepResult]]] = None,
    priority: int = 50,
    description: str = "",
) -> StepResult:
    """
    Define un paso de bypass/paso a través.

    Pasa los datos sin modificación. Útil para debugging, auditoría
    o como placeholder en un pipeline.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) que alimentan esta transformación
        priority: Prioridad de ejecución
        description: Descripción de la transformación

    Returns:
        StepResult que puede ser usado en pasos posteriores

    Example:
        >>> base = sql(name="Load", query="SELECT * FROM tabla")
        >>> sin_cambios = bypass(
        ...     name="Audit_Point",
        ...     inputs=base,
        ...     description="Punto de auditoría"
        ... )
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.TRANSFORMATION,
        class_name="BypassTransformStep",
        class_pretty_name="Bypass",
        arity=["UnaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
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


# ============================================================================
# PYTHON TRANSFORMATIONS
# ============================================================================


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
        step_type=StepType.TRANSFORMATION,
        class_name="PySparkTransformStep",
        class_pretty_name="PySpark",
        arity=["NaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "code": code,
            "isSaved": True,
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
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


# ============================================================================
# SQL TRANSFORMATIONS
# ============================================================================


def trigger(
    name: str,
    inputs: Optional[Union[StepResult, List[StepResult]]] = None,
    sql: str = "",
    quote_sql: bool = False,
    discard_conditions: str = "",
    replace_with_input_dataframe: bool = False,
    priority: int = 50,
    description: str = "",
) -> StepResult:
    """
    Define una transformación Trigger con SQL condicional.

    Ejecuta lógica SQL condicional que puede generar datos válidos o descartados.
    Permite bifurcar datos según condiciones específicas.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) que alimentan esta transformación
        sql: Query SQL a ejecutar
        quote_sql: Si se deben añadir comillas a la SQL
        discard_conditions: Condiciones para descartar datos
        replace_with_input_dataframe: Si reemplazar con el DataFrame de entrada
        priority: Prioridad de ejecución
        description: Descripción de la transformación

    Returns:
        StepResult que puede ser usado en pasos posteriores

    Example:
        >>> base = sql(name="Load", query="SELECT * FROM tabla")
        >>> trigger_step = trigger(
        ...     name="Filter_Valid",
        ...     inputs=base,
        ...     sql="SELECT * WHERE status IN ('active', 'pending')",
        ...     discard_conditions="status NOT IN ('active', 'pending')"
        ... )
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.TRANSFORMATION,
        class_name="TriggerTransformStep",
        class_pretty_name="Trigger",
        arity=["NaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "sql": sql,
            "quoteSql": quote_sql,
            "discardConditions": discard_conditions,
            "replaceWithInputDataframe": replace_with_input_dataframe,
            "genAIMetadataTablesDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Hybrid"],
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
