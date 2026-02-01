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
    description: str = "",
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
    description: str = "",
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
    description: str = "",
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
    description: str = "",
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
    description: str = "",
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
    description: str = "",
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

    if paths is None:
        paths = [
            {
                "path": None,
                "subdirGlobFilter": None,
                "subdirRegexFilter": None,
                "excludeGlobFilter": None,
                "excludeRegexFilter": None,
            }
        ]

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="ParquetInputStep",
        class_pretty_name="Parquet",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "path": path,
            "paths": paths,
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
        },
        supported_engines=["Batch", "Hybrid"],
    )

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
    description: str = "",
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

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="DeltaInputStep",
        class_pretty_name="Delta",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "path": path,
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
    description: str = "",
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

    if paths is None:
        paths = [
            {
                "path": None,
                "subdirGlobFilter": None,
                "subdirRegexFilter": None,
                "excludeGlobFilter": None,
                "excludeRegexFilter": None,
            }
        ]

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="JsonInputStep",
        class_pretty_name="Json",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "path": path,
            "paths": paths,
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
        },
        supported_engines=["Batch", "Hybrid"],
    )

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
    description: str = "",
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

    # Si no se proporcionan paths, crear una entrada por defecto
    if paths is None:
        paths = [
            {
                "path": None,
                "subdirGlobFilter": None,
                "subdirRegexFilter": None,
                "excludeGlobFilter": None,
                "excludeRegexFilter": None,
            }
        ]

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="CsvInputStep",
        class_pretty_name="Csv",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "path": path,
            "paths": paths,
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
        },
        supported_engines=["Batch", "Hybrid"],
    )

    pipeline.add_node(node)
    return StepResult(node, pipeline)


def filesystem(
    name: str,
    path: str = "",
    output_field: str = "raw",
    priority: int = 50,
    input_options: str = "",
    description: str = "",
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

    node = Node(
        name=name,
        step_type=StepType.INPUT,
        class_name="FileSystemInputStep",
        class_pretty_name="Filesystem",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "path": path,
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

    pipeline.add_node(node)
    return StepResult(node, pipeline)
