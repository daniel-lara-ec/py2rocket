"""
DSL Input Operations para Stratio Rocket

Define las operaciones de entrada (Input) disponibles en el DSL.
Están organizadas por categoría:

- CustomMade: CustomLiteXD
- Database: Jdbc, Postgres, SQL
- Python: PySpark
- StructuredFile: Delta, Parquet, Json
- UnstructuredFile: Csv, Filesystem

Estas funciones son los building blocks para entrada de datos en el pipeline.
"""

from typing import Optional, Union, List
from py2rocket.core.pipeline import (
    Node,
    StepType,
    ExecutionEngine,
    OutputWriter,
    StepResult,
    Pipeline,
    UIPosition,
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


def _apply_ui_position(
    node: Node, ui_position: Optional[Union[dict, "UIPosition"]]
) -> None:
    """Aplica la posición UI al nodo si se proporciona.

    Args:
        node: El nodo al que aplicar la posición
        ui_position: Diccionario con claves 'x' e 'y', o UIPosition object, o None
    """
    if ui_position is not None:

        if isinstance(ui_position, UIPosition):
            node.ui_configuration = ui_position.to_dict()
        elif isinstance(ui_position, dict):
            node.ui_configuration = ui_position
        else:
            raise TypeError(
                f"ui_position debe ser UIPosition o dict, recibido: {type(ui_position)}"
            )


def _apply_include_description(node: Node, include_description: bool) -> None:
    """Aplica el flag include_description al nodo si es False.

    Args:
        node: El nodo al que aplicar el flag
        include_description: Si incluir descripción en la serialización
    """
    if not include_description:
        node.include_description = False


def _process_outputs_writer(
    node: Node, outputs_writer: Optional[List[OutputWriter]]
) -> None:
    """Procesa la lista de OutputWriter y la asigna al nodo input."""
    if outputs_writer:
        node.outputs_writer = [ow.to_dict() for ow in outputs_writer]


def _sanitize_path(path: str) -> str:
    """Normaliza path removiendo saltos de línea y espacios extremos."""
    if not isinstance(path, str):
        return path
    return path.replace("\n", "").replace("\r", "").strip()


# ============================================================================
# CUSTOMADE INPUTS
# ============================================================================


def custom_lite_xd(
    name: str,
    custom_lite_class_type: str,
    priority: int = 50,
    vault_db_name: str = "",
    input_options: str = "",
    user_pass_enabled: bool = False,
    tls_enabled: bool = False,
    vault_custom_property_enabled: bool = False,
    is_streaming: bool = False,
    is_legacy_batch_step: bool = False,
    outputs_writer: Optional[List[OutputWriter]] = None,
    description: str = "",
    ui_position: Optional[Union[dict, "UIPosition"]] = None,
    include_description: bool = True,
) -> StepResult:
    """
    Define un paso de entrada CustomLiteXD personalizado.

    Permite usar extensiones personalizadas para leer datos de fuentes
    de datos específicas o legadas.

    Args:
        name: Nombre único del paso
        custom_lite_class_type: Tipo de clase CustomLite a usar
        priority: Prioridad de ejecución
        vault_db_name: Nombre de la BD del vault
        input_options: Opciones de entrada adicionales
        user_pass_enabled: Si se debe habilitar autenticación usuario/contraseña
        tls_enabled: Si se debe habilitar TLS
        vault_custom_property_enabled: Si se deben usar propiedades del vault
        is_streaming: Si es una operación de streaming
        is_legacy_batch_step: Si es un paso batch legado
        description: Descripción del propósito de este paso

    Returns:
        StepResult que puede ser usado como input en otros pasos
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="CustomLiteXDInputStep",
        class_pretty_name="CustomLiteXD",
        arity=["NullaryToNary"],
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
            "isStreaming": is_streaming,
            "isLegacyBatchStep": is_legacy_batch_step,
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "AutoInfer",
            },
        },
        supported_engines=["Hybrid"],
    )

    _apply_ui_position(node, ui_position)
    _apply_include_description(node, include_description)
    _process_outputs_writer(node, outputs_writer)
    pipeline.add_node(node)
    return StepResult(node, pipeline)


# ============================================================================
# SPECIAL INPUTS
# ============================================================================


def sftp_input(
    name: str,
    path: str = "",
    host: str = "",
    port: str = "22",
    username: str = "",
    password: str = "",
    file_type: str = "txt",
    avoid_hdfs_files: bool = False,
    tls_enabled: bool = False,
    vault_secret_name: str = "",
    vault_user_pass_enabled: bool = False,
    data_source_class: str = "",
    input_options: str = "",
    schema_spark_schema: str = "",
    priority: int = 50,
    outputs_writer: Optional[List[OutputWriter]] = None,
    description: str = "",
    ui_position: Optional[Union[dict, "UIPosition"]] = None,
    include_description: bool = True,
) -> StepResult:
    """
    Define un paso de entrada SFTP.

    Args:
        name: Nombre único del paso
        path: Ruta del archivo remoto
        host: Host del servidor SFTP
        port: Puerto SFTP (defecto 22)
        username: Usuario SFTP
        password: Contraseña SFTP
        file_type: Tipo de archivo (txt, csv, etc.)
        avoid_hdfs_files: Evitar archivos HDFS
        tls_enabled: Habilitar TLS
        vault_secret_name: Nombre de secreto en vault
        vault_user_pass_enabled: Habilitar credenciales de vault
        data_source_class: Clase datasource
        input_options: Opciones adicionales
        schema_spark_schema: Schema Spark
        priority: Prioridad de ejecución
        description: Descripción del paso
    """
    pipeline = get_current_pipeline()

    clean_path = _sanitize_path(path)

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="SFTPInputStep",
        class_pretty_name="SFTP",
        arity=["NullaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "inputOptions": input_options,
            "path": clean_path,
            "username": username,
            "password": password,
            "host": host,
            "port": port,
            "fileType": file_type,
            "avoidHdfsFiles": avoid_hdfs_files,
            "tlsEnabled": tls_enabled,
            "vaultSecretName": vault_secret_name,
            "vaultUserPassEnabled": vault_user_pass_enabled,
            "dataSourceClass": data_source_class,
            "schema.sparkSchema": schema_spark_schema,
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
        },
        supported_engines=["Batch", "Hybrid", "Streaming"],
    )

    _apply_ui_position(node, ui_position)
    _apply_include_description(node, include_description)
    _process_outputs_writer(node, outputs_writer)
    pipeline.add_node(node)
    return StepResult(node, pipeline)


def test_input(
    name: str,
    event_type: str = "STRING",
    event: str = "",
    output_field: str = "raw",
    num_events: str = "10",
    max_number: str = "",
    explode_event: bool = False,
    priority: int = 50,
    outputs_writer: Optional[List[OutputWriter]] = None,
    description: str = "",
    ui_position: Optional[Union[dict, "UIPosition"]] = None,
    include_description: bool = True,
) -> StepResult:
    """
    Define un paso de entrada Test.

    Args:
        name: Nombre único del paso
        event_type: Tipo de evento
        event: Evento
        output_field: Campo de salida
        num_events: Número de eventos
        max_number: Máximo número
        explode_event: Si explotar el evento
        priority: Prioridad de ejecución
        description: Descripción del paso
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="TestInputStep",
        class_pretty_name="Test",
        arity=["NullaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "eventType": event_type,
            "event": event,
            "outputField": output_field,
            "numEvents": num_events,
            "maxNumber": max_number,
            "explodeEvent": explode_event,
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
        },
        supported_engines=["Batch", "Hybrid", "Streaming"],
    )

    _apply_ui_position(node, ui_position)
    _apply_include_description(node, include_description)
    _process_outputs_writer(node, outputs_writer)
    pipeline.add_node(node)
    return StepResult(node, pipeline)


# ============================================================================
# DATABASE INPUTS
# ============================================================================


def sql(
    name: str,
    query: str,
    priority: int = 50,
    cache_table: bool = False,
    force_native_query: bool = False,
    outputs_writer: Optional[List[OutputWriter]] = None,
    description: str = "",
    ui_position: Optional[Union[dict, "UIPosition"]] = None,
    include_description: bool = True,
) -> StepResult:
    """
    Define un paso de entrada SQL.

    Ejecuta una query SQL sobre las fuentes de datos configuradas en Rocket.
    Soporta parámetros mediante sintaxis {{{NOMBRE_PARAMETRO}}}.

    Args:
        name: Nombre único del paso
        query: Query SQL a ejecutar. Puede incluir parámetros: {{{P_TABLA}}}
        priority: Prioridad de ejecución (menor número = ejecuta primero)
        cache_table: Si se debe cachear el resultado en memoria
        force_native_query: Forzar ejecución nativa de la query
        description: Descripción del propósito de este paso

    Returns:
        StepResult que puede ser usado como input en otros pasos

    Example:
        >>> tabla = sql(
        ...     name="Load_Ventas",
        ...     query="SELECT * FROM {{{P_TABLA}}} WHERE fecha >= '2024-01-01'",
        ...     priority=10
        ... )
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="SQLInputStep",
        class_pretty_name="SQL",
        arity=["NullaryToNary"],
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

    _apply_ui_position(node, ui_position)
    _apply_include_description(node, include_description)
    _process_outputs_writer(node, outputs_writer)
    pipeline.add_node(node)
    return StepResult(node, pipeline)


def jdbc(
    name: str,
    url: str = "",
    dbtable: str = "",
    driver: str = "org.postgresql.Driver",
    priority: int = 50,
    isolation_level: str = "READ_UNCOMMITTED",
    tls_enabled: bool = False,
    user_pass_enabled: bool = False,
    vault_db_name: str = "",
    input_options: str = "",
    outputs_writer: Optional[List[OutputWriter]] = None,
    description: str = "",
    ui_position: Optional[Union[dict, "UIPosition"]] = None,
    include_description: bool = True,
) -> StepResult:
    """
    Define un paso de entrada JDBC.

    Lee datos desde una base de datos mediante conexión JDBC (compatible con
    PostgreSQL, MySQL, Oracle, SQL Server, etc.).

    Args:
        name: Nombre único del paso
        url: URL de conexión JDBC
        dbtable: Tabla o query a leer (ej: "schema.table" o "(SELECT ...) AS t")
        driver: Driver JDBC a usar (defecto: PostgreSQL)
        priority: Prioridad de ejecución
        isolation_level: Nivel de aislamiento (READ_UNCOMMITTED, etc.)
        tls_enabled: Si se debe habilitar TLS
        user_pass_enabled: Si se debe habilitar autenticación usuario/contraseña
        vault_db_name: Nombre de la BD del vault para credenciales
        input_options: Opciones adicionales de JDBC
        description: Descripción del propósito de este paso

    Returns:
        StepResult que puede ser usado como input en otros pasos

    Example:
        >>> tabla = jdbc(
        ...     name="Load_Usuarios",
        ...     url="jdbc:postgresql://localhost:5432/mydb",
        ...     dbtable="public.usuarios",
        ...     driver="org.postgresql.Driver"
        ... )
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="JdbcInputStep",
        class_pretty_name="Jdbc",
        arity=["NullaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "url": url,
            "dbtable": dbtable,
            "driver": driver,
            "isolationLevel": isolation_level,
            "tlsEnabled": tls_enabled,
            "userPassEnabled": user_pass_enabled,
            "vaultDbName": vault_db_name,
            "inputOptions": input_options,
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "AutoInfer",
            },
        },
        supported_engines=["Batch", "Hybrid"],
    )

    _apply_ui_position(node, ui_position)
    _apply_include_description(node, include_description)
    _process_outputs_writer(node, outputs_writer)
    pipeline.add_node(node)
    return StepResult(node, pipeline)


def postgres(
    name: str,
    url: str = "",
    dbtable: str = "",
    select_input: str = "TABLE",
    select_exp: str = "",
    priority: int = 50,
    isolation_level: str = "READ_UNCOMMITTED",
    tls_enabled: bool = True,
    case_sensitive_enabled: bool = True,
    outputs_writer: Optional[List[OutputWriter]] = None,
    description: str = "",
    ui_position: Optional[Union[dict, "UIPosition"]] = None,
    include_description: bool = True,
) -> StepResult:
    """
    Define un paso de entrada PostgreSQL.

    Lee datos directamente desde una base de datos PostgreSQL.

    Args:
        name: Nombre único del paso
        url: URL de conexión PostgreSQL (ej: "jdbc:postgresql://host:5432/db")
        dbtable: Tabla a leer (ej: "schema.table")
        select_input: Tipo de entrada ("TABLE" o "EXPRESSION")
        select_exp: Expresión SQL si select_input="EXPRESSION"
        priority: Prioridad de ejecución
        isolation_level: Nivel de aislamiento de transacciones
        tls_enabled: Si se debe habilitar TLS
        case_sensitive_enabled: Si los nombres son sensibles a mayúsculas
        description: Descripción del propósito de este paso

    Returns:
        StepResult que puede ser usado como input en otros pasos

    Example:
        >>> tabla = postgres(
        ...     name="Load_Pedidos",
        ...     url="jdbc:postgresql://localhost:5432/mydb",
        ...     dbtable="public.pedidos"
        ... )
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="PostgresInputStep",
        class_pretty_name="Postgres",
        arity=["NullaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "url": url,
            "dbtable": dbtable,
            "selectInput": select_input,
            "selectExp": select_exp,
            "isolationLevel": isolation_level,
            "tlsEnabled": tls_enabled,
            "caseSensitiveEnabled": case_sensitive_enabled,
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "AutoInfer",
            },
        },
        supported_engines=["Batch", "Hybrid"],
    )

    _apply_ui_position(node, ui_position)
    _apply_include_description(node, include_description)
    _process_outputs_writer(node, outputs_writer)
    pipeline.add_node(node)
    return StepResult(node, pipeline)


# ============================================================================
# PYTHON INPUTS
# ============================================================================


def pyspark_input(
    name: str,
    python_code: str,
    python_input_dictionary: str = "",
    priority: int = 50,
    outputs_writer: Optional[List[OutputWriter]] = None,
    description: str = "",
    ui_position: Optional[Union[dict, "UIPosition"]] = None,
    include_description: bool = True,
) -> StepResult:
    """
    Define un paso de entrada PySpark personalizado.

    Ejecuta código Python para generar un DataFrame desde cualquier fuente
    de datos personalizada.

    Args:
        name: Nombre único del paso
        python_code: Código Python a ejecutar. Debe devolver un DataFrame
        python_input_dictionary: Diccionario de entrada con variables disponibles
        priority: Prioridad de ejecución
        description: Descripción del propósito de este paso

    Returns:
        StepResult que puede ser usado como input en otros pasos

    Example:
        >>> entrada = pyspark_input(
        ...     name="Generate_Data",
        ...     python_code="spark.createDataFrame([(1, 'a'), (2, 'b')], ['id', 'name'])"
        ... )
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="PySparkInputStep",
        class_pretty_name="PySpark",
        arity=["NullaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "pythonCode": python_code,
            "pythonInputDictionary": python_input_dictionary,
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "AutoInfer",
            },
        },
        supported_engines=["Batch", "Hybrid"],
    )

    _apply_ui_position(node, ui_position)
    _apply_include_description(node, include_description)
    _process_outputs_writer(node, outputs_writer)
    pipeline.add_node(node)
    return StepResult(node, pipeline)


# ============================================================================
# STRUCTURED FILE INPUTS
# ============================================================================


def parquet(
    name: str,
    path: str = "",
    paths: Optional[list] = None,
    priority: int = 50,
    path_glob_filter: str = "*.parquet",
    is_recursive_enabled: bool = True,
    metadata_column_enabled: bool = True,
    enable_filter_pattern: bool = True,
    outputs_writer: Optional[List[OutputWriter]] = None,
    description: str = "",
    ui_position: Optional[Union[dict, "UIPosition"]] = None,
    include_description: bool = True,
) -> StepResult:
    """
    Define un paso de entrada Parquet.

    Lee archivos Parquet desde el sistema de archivos o almacenamiento.

    Args:
        name: Nombre único del paso
        path: Ruta al archivo o directorio Parquet
        paths: Lista de rutas múltiples con configuraciones
        priority: Prioridad de ejecución
        path_glob_filter: Patrón glob para filtrar archivos
        is_recursive_enabled: Si se debe buscar recursivamente
        metadata_column_enabled: Si se deben incluir columnas de metadatos
        enable_filter_pattern: Si se debe habilitar filtrado por patrón
        description: Descripción del propósito de este paso

    Returns:
        StepResult que puede ser usado como input en otros pasos
    """
    pipeline = get_current_pipeline()

    clean_path = _sanitize_path(path)
    configuration = {
        "path": clean_path,
        "pathGlobFilter": path_glob_filter,
        "isRecursiveEnabled": is_recursive_enabled,
        "metadataColumnEnabled": metadata_column_enabled,
        "enableFilterPattern": enable_filter_pattern,
        "readMode": "DefaultReadMode",
        "genAIMetadataTableDescription": "",
        "genAIMetadataColumns": "",
        "debugOptions": {
            "executeStepAutoDebug": True,
            "executeStepDebug": True,
            "mockType": "AutoInfer",
        },
    }
    if isinstance(paths, list):
        configuration["paths"] = paths

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="ParquetInputStep",
        class_pretty_name="Parquet",
        arity=["NullaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration=configuration,
        supported_engines=["Batch", "Hybrid"],
    )

    _apply_ui_position(node, ui_position)
    _apply_include_description(node, include_description)
    _process_outputs_writer(node, outputs_writer)
    pipeline.add_node(node)
    return StepResult(node, pipeline)


def delta(
    name: str,
    path: str = "",
    priority: int = 50,
    enable_reading_older_versions: bool = False,
    read_older_version_by: str = "versionAsOf",
    version_as_of: str = "",
    timestamp_as_of: str = "",
    outputs_writer: Optional[List[OutputWriter]] = None,
    description: str = "",
    ui_position: Optional[Union[dict, "UIPosition"]] = None,
    include_description: bool = True,
) -> StepResult:
    """
    Define un paso de entrada Delta Lake.

    Lee tablas Delta Lake desde el almacenamiento DBFS o S3.
    Soporta lectura de versiones anteriores con time-travel.

    Args:
        name: Nombre único del paso
        path: Ruta a la tabla Delta
        priority: Prioridad de ejecución
        enable_reading_older_versions: Si se debe habilitar time-travel
        read_older_version_by: Criterio ("versionAsOf" o "timestampAsOf")
        version_as_of: Versión específica a leer (si read_older_version_by="versionAsOf")
        timestamp_as_of: Timestamp específico (si read_older_version_by="timestampAsOf")
        description: Descripción del propósito de este paso

    Returns:
        StepResult que puede ser usado como input en otros pasos

    Example:
        >>> tabla = delta(
        ...     name="Load_Delta",
        ...     path="/mnt/data/delta_table",
        ...     enable_reading_older_versions=True,
        ...     version_as_of="5"
        ... )
    """
    pipeline = get_current_pipeline()

    clean_path = _sanitize_path(path)

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="DeltaInputStep",
        class_pretty_name="Delta",
        arity=["NullaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "path": clean_path,
            "enableReadingOfOlderVersions": enable_reading_older_versions,
            "readOlderVersionBy": read_older_version_by,
            "versionAsOf": version_as_of,
            "timestampAsOf": timestamp_as_of,
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "AutoInfer",
            },
        },
        supported_engines=["Batch", "Hybrid"],
    )

    _apply_ui_position(node, ui_position)
    _apply_include_description(node, include_description)
    _process_outputs_writer(node, outputs_writer)
    pipeline.add_node(node)
    return StepResult(node, pipeline)


def json(
    name: str,
    path: str = "",
    paths: Optional[list] = None,
    priority: int = 50,
    path_glob_filter: str = "*.json",
    is_recursive_enabled: bool = True,
    metadata_column_enabled: bool = True,
    enable_filter_pattern: bool = True,
    multiline_enabled: bool = False,
    outputs_writer: Optional[List[OutputWriter]] = None,
    description: str = "",
    ui_position: Optional[Union[dict, "UIPosition"]] = None,
    include_description: bool = True,
) -> StepResult:
    """
    Define un paso de entrada JSON.

    Lee archivos JSON desde el sistema de archivos o almacenamiento.
    Soporta JSON de línea única y multiline.

    Args:
        name: Nombre único del paso
        path: Ruta al archivo o directorio JSON
        paths: Lista de rutas múltiples
        priority: Prioridad de ejecución
        path_glob_filter: Patrón glob para filtrar archivos
        is_recursive_enabled: Si se debe buscar recursivamente
        metadata_column_enabled: Si se deben incluir columnas de metadatos
        enable_filter_pattern: Si se debe habilitar filtrado por patrón
        multiline_enabled: Si se debe habilitar JSON multiline
        description: Descripción del propósito de este paso

    Returns:
        StepResult que puede ser usado como input en otros pasos
    """
    pipeline = get_current_pipeline()

    clean_path = _sanitize_path(path)
    configuration = {
        "path": clean_path,
        "pathGlobFilter": path_glob_filter,
        "isRecursiveEnabled": is_recursive_enabled,
        "metadataColumnEnabled": metadata_column_enabled,
        "enableFilterPattern": enable_filter_pattern,
        "multilineEnabled": multiline_enabled,
        "readMode": "DefaultReadMode",
        "genAIMetadataTableDescription": "",
        "genAIMetadataColumns": "",
        "debugOptions": {
            "executeStepAutoDebug": True,
            "executeStepDebug": True,
            "mockType": "AutoInfer",
        },
    }
    if isinstance(paths, list):
        configuration["paths"] = paths

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="JsonInputStep",
        class_pretty_name="Json",
        arity=["NullaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration=configuration,
        supported_engines=["Batch", "Hybrid"],
    )

    _apply_ui_position(node, ui_position)
    _apply_include_description(node, include_description)
    _process_outputs_writer(node, outputs_writer)
    pipeline.add_node(node)
    return StepResult(node, pipeline)


# ============================================================================
# UNSTRUCTURED FILE INPUTS
# ============================================================================


def csv(
    name: str,
    path: str = "",
    paths: Optional[list] = None,
    delimiter: str = ",",
    header: bool = False,
    priority: int = 50,
    path_glob_filter: str = "*.csv",
    is_recursive_enabled: bool = True,
    metadata_column_enabled: bool = True,
    data_as_json_enabled: bool = True,
    enable_filter_pattern: bool = True,
    outputs_writer: Optional[List[OutputWriter]] = None,
    description: str = "",
    ui_position: Optional[Union[dict, "UIPosition"]] = None,
    include_description: bool = True,
) -> StepResult:
    """
    Define un paso de entrada CSV.

    Lee archivos CSV desde el sistema de archivos o almacenamiento.

    Args:
        name: Nombre único del paso
        path: Ruta al archivo o directorio CSV
        paths: Lista de rutas múltiples con configuraciones avanzadas
        delimiter: Delimitador de campos (por defecto: ',')
        header: Si la primera fila contiene encabezados
        priority: Prioridad de ejecución (menor número = ejecuta primero)
        path_glob_filter: Patrón glob para filtrar archivos
        is_recursive_enabled: Si se debe buscar recursivamente en subdirectorios
        metadata_column_enabled: Si se deben incluir columnas de metadatos
        data_as_json_enabled: Si se debe habilitar lectura como JSON
        enable_filter_pattern: Si se debe habilitar el filtrado por patrón
        description: Descripción del propósito de este paso

    Returns:
        StepResult que puede ser usado como input en otros pasos

    Example:
        >>> datos = csv(
        ...     name="Load_CSV",
        ...     path="/data/ventas.csv",
        ...     header=True,
        ...     delimiter=","
        ... )
    """
    pipeline = get_current_pipeline()

    clean_path = _sanitize_path(path)
    configuration = {
        "path": clean_path,
        "delimiter": delimiter,
        "header": header,
        "pathGlobFilter": path_glob_filter,
        "isRecursiveEnabled": is_recursive_enabled,
        "metadataColumnEnabled": metadata_column_enabled,
        "dataAsJsonEnabled": data_as_json_enabled,
        "enableFilterPattern": enable_filter_pattern,
        "readMode": "DefaultReadMode",
        "excludeGlobFilter": "",
        "excludeRegexFilter": "",
        "subdirGlobFilter": "",
        "subdirRegexFilter": "",
        "inputOptions": "",
        "schema.inputMode": "NOSCHEMAPROVIDED",
        "schema.header": "",
        "schema.fields": "",
        "schema.sparkSchema": "",
        "genAIMetadataTableDescription": "",
        "genAIMetadataColumns": "",
        "debugOptions": {
            "executeStepAutoDebug": True,
            "executeStepDebug": True,
            "mockType": "AutoInfer",
        },
    }
    if isinstance(paths, list):
        configuration["paths"] = paths

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="CsvInputStep",
        class_pretty_name="Csv",
        arity=["NullaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration=configuration,
        supported_engines=["Batch", "Hybrid"],
    )

    _apply_ui_position(node, ui_position)
    _apply_include_description(node, include_description)
    _process_outputs_writer(node, outputs_writer)
    pipeline.add_node(node)
    return StepResult(node, pipeline)


def filesystem(
    name: str,
    path: str = "",
    output_field: str = "raw",
    priority: int = 50,
    input_options: str = "",
    outputs_writer: Optional[List[OutputWriter]] = None,
    description: str = "",
    ui_position: Optional[Union[dict, "UIPosition"]] = None,
    include_description: bool = True,
) -> StepResult:
    """
    Define un paso de entrada Filesystem.

    Lee archivos de texto directamente del sistema de archivos.
    Retorna el contenido bruto del archivo como un campo.

    Args:
        name: Nombre único del paso
        path: Ruta al archivo o directorio
        output_field: Campo de salida donde se almacena el contenido
        priority: Prioridad de ejecución
        input_options: Opciones adicionales de entrada
        description: Descripción del propósito de este paso

    Returns:
        StepResult que puede ser usado como input en otros pasos

    Example:
        >>> archivos = filesystem(
        ...     name="Load_Files",
        ...     path="/data/logs",
        ...     output_field="content"
        ... )
    """
    pipeline = get_current_pipeline()

    clean_path = _sanitize_path(path)

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="FileSystemInputStep",
        class_pretty_name="Filesystem",
        arity=["NullaryToNary"],
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "path": clean_path,
            "outputField": output_field,
            "inputOptions": input_options,
            "genAIMetadataTableDescription": "",
            "genAIMetadataColumns": "",
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "AutoInfer",
            },
        },
        supported_engines=["Batch", "Hybrid"],
    )

    _apply_ui_position(node, ui_position)
    _apply_include_description(node, include_description)
    _process_outputs_writer(node, outputs_writer)
    pipeline.add_node(node)
    return StepResult(node, pipeline)
