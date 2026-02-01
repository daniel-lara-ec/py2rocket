"""
DSL Output Operations para Stratio Rocket

Define las operaciones de salida (Output) disponibles en el DSL.
Están organizadas por categoría:

- CustomMade: CustomLiteXD
- Database: Jdbc, Postgres, Sftp
- Other: Print, RunWorkflow
- Python: PySpark
- StructuredFile: Delta, Parquet, Json
- UnstructuredFile: Csv, Text

Estas funciones definen las salidas del pipeline.
"""

from typing import Optional, Union, List, Dict, Any
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


def _attach_outputs_writer(
    input_step: StepResult,
    output_step_name: str,
    save_mode: Optional[str] = None,
    table_name: str = "",
    discard_table_name: str = "",
    extra_options: Optional[Dict[str, Any]] = None,
) -> None:
    """Adjunta configuración outputsWriter al nodo de transformación origen."""
    if input_step.node.step_type != StepType.TRANSFORMATION:
        return

    if save_mode is None:
        save_mode = "Overwrite"

    if input_step.node.outputs_writer is None:
        input_step.node.outputs_writer = []

    for ow in input_step.node.outputs_writer:
        if ow.get("outputStepName") == output_step_name:
            return
        if ow.get("outputStepName") in (None, ""):
            ow["outputStepName"] = output_step_name
            return

    if extra_options is None:
        extra_options = {
            "checkIfEmpty": False,
            "partitionBy": "overwrite",
            "partitionOverwriteEnabled": True,
            "partitionColumns": "",
            "saveMode": save_mode,
            "partitions": "",
        }
    else:
        extra_options = dict(extra_options)
        extra_options.setdefault("saveMode", save_mode)

    input_step.node.outputs_writer.append(
        {
            "saveMode": save_mode,
            "outputStepName": output_step_name,
            "tableName": table_name,
            "discardTableName": discard_table_name,
            "extraOptions": extra_options,
        }
    )


# ============================================================================
# CUSTOMMADE OUTPUTS
# ============================================================================


def custom_lite_xd_output(
    name: str,
    inputs: Union[StepResult, List[StepResult]],
    custom_lite_class_type: str,
    priority: int = 50,
    vault_db_name: str = "",
    output_options: str = "",
    user_pass_enabled: bool = False,
    tls_enabled: bool = False,
    vault_custom_property_enabled: bool = False,
    description: str = "",
) -> StepResult:
    """
    Define un paso de salida CustomLiteXD personalizado.

    Permite usar extensiones personalizadas para escribir datos en destinos
    específicos o legados.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) del cual se escribirán los datos
        custom_lite_class_type: Tipo de clase CustomLite a usar
        priority: Prioridad de ejecución
        vault_db_name: Nombre de la BD del vault
        output_options: Opciones de salida adicionales
        user_pass_enabled: Si se debe habilitar autenticación usuario/contraseña
        tls_enabled: Si se debe habilitar TLS
        vault_custom_property_enabled: Si se deben usar propiedades del vault
        description: Descripción del propósito de este paso

    Returns:
        StepResult del paso output
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.OUTPUT,
        class_name="CustomLiteXDOutputStep",
        class_pretty_name="CustomLiteXD",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "customLiteClassType": custom_lite_class_type,
            "vaultDbName": vault_db_name,
            "outputOptions": output_options,
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

    # Manejar múltiples inputs
    if isinstance(inputs, list):
        for input_step in inputs:
            edge = Edge(
                origin=input_step.node.name,
                destination=node.name,
                data_type=DataRelation.VALID_DATA,
            )
            pipeline.add_edge(edge)
            _attach_outputs_writer(input_step, node.name, "Overwrite")
    else:
        edge = Edge(
            origin=inputs.node.name,
            destination=node.name,
            data_type=DataRelation.VALID_DATA,
        )
        pipeline.add_edge(edge)
        _attach_outputs_writer(inputs, node.name, "Overwrite")

    return StepResult(node, pipeline)


# ============================================================================
# DATABASE OUTPUTS
# ============================================================================


def jdbc_output(
    name: str,
    inputs: Union[StepResult, List[StepResult]],
    url: str = "",
    dbtable: str = "",
    driver: str = "",
    priority: int = 50,
    batch_size: str = "1000",
    isolation_level: str = "READ_UNCOMMITTED",
    tls_enabled: bool = False,
    user_pass_enabled: bool = False,
    fail_fast: bool = True,
    case_sensitive_enabled: bool = True,
    create_schema_if_not_exists: bool = False,
    jdbc_save_mode: str = "STATEMENT",
    schema_from_database: bool = False,
    save_options: str = "",
    description: str = "",
) -> StepResult:
    """
    Define un paso de salida JDBC.

    Escribe datos a una base de datos mediante conexión JDBC.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) del cual se escribirán los datos
        url: URL de conexión JDBC
        dbtable: Tabla destino
        driver: Driver JDBC a usar
        priority: Prioridad de ejecución
        batch_size: Tamaño del lote para escritura
        isolation_level: Nivel de aislamiento
        tls_enabled: Si se debe habilitar TLS
        user_pass_enabled: Si se debe habilitar autenticación
        fail_fast: Si fallar rápidamente en errores
        case_sensitive_enabled: Si sensible a mayúsculas
        create_schema_if_not_exists: Si crear schema si no existe
        jdbc_save_mode: Modo de guardado (STATEMENT, etc.)
        schema_from_database: Si obtener schema de la BD
        save_options: Opciones adicionales
        description: Descripción del propósito de este paso

    Returns:
        StepResult del paso output
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.OUTPUT,
        class_name="JdbcOutputStep",
        class_pretty_name="Jdbc",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "url": url,
            "dbtable": dbtable,
            "driver": driver,
            "batchsize": batch_size,
            "isolationLevel": isolation_level,
            "tlsEnabled": tls_enabled,
            "userPassEnable": user_pass_enabled,
            "failFast": fail_fast,
            "caseSensitiveEnabled": case_sensitive_enabled,
            "createSchemaIfNotExists": create_schema_if_not_exists,
            "jdbcSaveMode": jdbc_save_mode,
            "schemaFromDatabase": schema_from_database,
            "saveOptions": save_options,
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
    )

    pipeline.add_node(node)

    # Manejar múltiples inputs
    if isinstance(inputs, list):
        for input_step in inputs:
            edge = Edge(
                origin=input_step.node.name,
                destination=node.name,
                data_type=DataRelation.VALID_DATA,
            )
            pipeline.add_edge(edge)
            _attach_outputs_writer(
                input_step, node.name, jdbc_save_mode, table_name=dbtable
            )
    else:
        edge = Edge(
            origin=inputs.node.name,
            destination=node.name,
            data_type=DataRelation.VALID_DATA,
        )
        pipeline.add_edge(edge)
        _attach_outputs_writer(inputs, node.name, jdbc_save_mode, table_name=dbtable)

    return StepResult(node, pipeline)


def postgres_output(
    name: str,
    inputs: Union[StepResult, List[StepResult]],
    url: str = "",
    dbtable: str = "",
    priority: int = 50,
    batch_size: str = "1000",
    isolation_level: str = "READ_UNCOMMITTED",
    tls_enabled: bool = True,
    create_schema_if_not_exists: bool = False,
    save_options: str = "",
    description: str = "",
) -> StepResult:
    """
    Define un paso de salida PostgreSQL.

    Escribe datos a una base de datos PostgreSQL.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) del cual se escribirán los datos
        url: URL de conexión PostgreSQL
        dbtable: Tabla destino
        priority: Prioridad de ejecución
        batch_size: Tamaño del lote para escritura
        isolation_level: Nivel de aislamiento
        tls_enabled: Si se debe habilitar TLS
        create_schema_if_not_exists: Si crear schema si no existe
        save_options: Opciones adicionales
        description: Descripción del propósito de este paso

    Returns:
        StepResult del paso output
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.OUTPUT,
        class_name="PostgresOutputStep",
        class_pretty_name="Postgres",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "url": url,
            "dbtable": dbtable,
            "batchsize": batch_size,
            "isolationLevel": isolation_level,
            "tlsEnabled": tls_enabled,
            "createSchemaIfNotExists": create_schema_if_not_exists,
            "saveOptions": save_options,
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
    )

    pipeline.add_node(node)

    # Manejar múltiples inputs
    if isinstance(inputs, list):
        for input_step in inputs:
            edge = Edge(
                origin=input_step.node.name,
                destination=node.name,
                data_type=DataRelation.VALID_DATA,
            )
            pipeline.add_edge(edge)
            _attach_outputs_writer(input_step, node.name)
    else:
        edge = Edge(
            origin=inputs.node.name,
            destination=node.name,
            data_type=DataRelation.VALID_DATA,
        )
        pipeline.add_edge(edge)
        _attach_outputs_writer(inputs, node.name)

    return StepResult(node, pipeline)


def sftp_output(
    name: str,
    inputs: Union[StepResult, List[StepResult]],
    host: str = "",
    port: str = "22",
    path: str = "",
    priority: int = 50,
    file_type: str = "txt",
    custom_file_type: str = "",
    sftp_server_username: str = "",
    password: str = "",
    tls_enabled: bool = False,
    preserve_writer_file_extension: bool = False,
    avoid_hdfs_files: bool = False,
    vault_secret_name: str = "",
    vault_user_pass_enabled: bool = False,
    data_source_class: str = "",
    save_options: str = "",
    description: str = "",
) -> StepResult:
    """
    Define un paso de salida SFTP.

    Escribe datos a un servidor SFTP.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) del cual se escribirán los datos
        host: Host del servidor SFTP
        port: Puerto SFTP (defecto 22)
        path: Ruta destino en el servidor
        priority: Prioridad de ejecución
        file_type: Tipo de archivo (txt, csv, etc.)
        custom_file_type: Tipo de archivo personalizado
        sftp_server_username: Usuario del servidor SFTP
        password: Contraseña (si no se usa vault)
        tls_enabled: Si se debe habilitar TLS
        preserve_writer_file_extension: Si preservar extensión de archivo
        avoid_hdfs_files: Si evitar archivos HDFS
        vault_secret_name: Nombre del secreto en el vault
        vault_user_pass_enabled: Si usar credenciales del vault
        data_source_class: Clase de fuente de datos personalizada
        save_options: Opciones adicionales
        description: Descripción del propósito de este paso

    Returns:
        StepResult del paso output
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.OUTPUT,
        class_name="SFTPOutputStep",
        class_pretty_name="SFTP",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "host": host,
            "port": port,
            "path": path,
            "fileType": file_type,
            "customFileType": custom_file_type,
            "sftpServerUsername": sftp_server_username,
            "password": password,
            "tlsEnabled": tls_enabled,
            "preserveWriterFileExtension": preserve_writer_file_extension,
            "avoidHdfsFiles": avoid_hdfs_files,
            "vaultSecretName": vault_secret_name,
            "vaultUserPassEnabled": vault_user_pass_enabled,
            "dataSourceClass": data_source_class,
            "saveOptions": save_options,
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
    )

    pipeline.add_node(node)

    # Manejar múltiples inputs
    if isinstance(inputs, list):
        for input_step in inputs:
            edge = Edge(
                origin=input_step.node.name,
                destination=node.name,
                data_type=DataRelation.VALID_DATA,
            )
            pipeline.add_edge(edge)
            _attach_outputs_writer(input_step, node.name)
    else:
        edge = Edge(
            origin=inputs.node.name,
            destination=node.name,
            data_type=DataRelation.VALID_DATA,
        )
        pipeline.add_edge(edge)
        _attach_outputs_writer(inputs, node.name)

    return StepResult(node, pipeline)


# ============================================================================
# OTHER OUTPUTS
# ============================================================================


def print_step(
    name: str,
    inputs: Union[StepResult, List[StepResult]],
    priority: int = 50,
    print_data: bool = False,
    print_schema: bool = False,
    print_metadata: bool = True,
    log_level: str = "warn",
    description: str = "",
) -> StepResult:
    """
    Define un paso de salida para imprimir/mostrar datos.

    Útil para debugging y validación. Imprime información sobre el DataFrame
    sin persistirlo.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) del cual se imprimirán los datos
        priority: Prioridad de ejecución
        print_data: Si se deben imprimir los datos (puede ser costoso)
        print_schema: Si se debe imprimir el schema del DataFrame
        print_metadata: Si se deben imprimir metadatos (filas, columnas, etc.)
        log_level: Nivel de log (debug, info, warn, error)
        description: Descripción del propósito de este paso

    Returns:
        StepResult del paso print

    Example:
        >>> tabla = sql(name="Load", query="SELECT * FROM tabla")
        >>> print_step(name="PrintInfo", inputs=tabla, print_schema=True)
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.OUTPUT,
        class_name="PrintOutputStep",
        class_pretty_name="Print",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "printData": print_data,
            "printSchema": print_schema,
            "printMetadata": print_metadata,
            "logLevel": log_level,
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
    )

    pipeline.add_node(node)

    # Manejar múltiples inputs
    if isinstance(inputs, list):
        for input_step in inputs:
            edge = Edge(
                origin=input_step.node.name,
                destination=node.name,
                data_type=DataRelation.VALID_DATA,
            )
            pipeline.add_edge(edge)
    else:
        edge = Edge(
            origin=inputs.node.name,
            destination=node.name,
            data_type=DataRelation.VALID_DATA,
        )
        pipeline.add_edge(edge)

    return StepResult(node, pipeline)


def run_workflow(
    name: str,
    inputs: Optional[Union[StepResult, List[StepResult]]] = None,
    workflow_id: str = "",
    asset_id: str = "",
    priority: int = 50,
    execution_priority: int = 0,
    run_workflow_when: str = "RECEIVE_DATA",
    variables: str = "",
    contexts: str = "",
    forward_variables: bool = False,
    forward_contexts: bool = False,
    unique_instance: bool = False,
    drop_duplicates: bool = False,
    limit_max_input_rows: str = "",
    max_attempts: int = 0,
    attempts_conditions: str = "",
    force_execution_if_available_resources: bool = False,
    retry_unsuccessful_writes: bool = False,
    description: str = "",
) -> StepResult:
    """
    Define un paso de salida para ejecutar otro workflow.

    Permite invocar workflows adicionales como parte del pipeline,
    facilitando la composición de workflows complejos.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) que alimentan esta salida
        workflow_id: ID del workflow a ejecutar
        asset_id: ID del asset asociado
        priority: Prioridad de ejecución del paso
        execution_priority: Prioridad de ejecución del workflow invocado
        run_workflow_when: Cuándo ejecutar ("RECEIVE_DATA", etc.)
        variables: Variables a pasar al workflow
        contexts: Contextos a pasar al workflow
        forward_variables: Si se deben reenviar las variables del workflow actual
        forward_contexts: Si se deben reenviar los contextos del workflow actual
        unique_instance: Si se debe ejecutar una única instancia
        drop_duplicates: Si se deben eliminar duplicados antes de ejecutar
        limit_max_input_rows: Límite máximo de filas de entrada
        max_attempts: Número máximo de intentos
        attempts_conditions: Condiciones para reintentos
        force_execution_if_available_resources: Forzar ejecución si hay recursos
        retry_unsuccessful_writes: Reintentar escrituras fallidas
        description: Descripción del propósito de este paso

    Returns:
        StepResult del paso run_workflow

    Example:
        >>> tabla = sql(name="Load", query="SELECT * FROM tabla")
        >>> run_workflow(
        ...     name="Execute_Process",
        ...     inputs=tabla,
        ...     workflow_id="workflow-123",
        ...     run_workflow_when="RECEIVE_DATA"
        ... )
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.OUTPUT,
        class_name="RunWorkflowOutputStep",
        class_pretty_name="RunWorkflow",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "workflowId": workflow_id,
            "assetId": asset_id,
            "executionPriority": str(execution_priority),
            "runWorkflowWhen": run_workflow_when,
            "variables": variables,
            "contexts": contexts,
            "forwardVariables": forward_variables,
            "forwardContexts": forward_contexts,
            "uniqueInstance": unique_instance,
            "dropDuplicates": drop_duplicates,
            "limitMaxInputRows": limit_max_input_rows,
            "maxAttempts": str(max_attempts),
            "attemptsConditions": attempts_conditions,
            "forceExecutionIfAvailableResources": force_execution_if_available_resources,
            "retryUnsuccessfulWrites": retry_unsuccessful_writes,
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
# PYTHON OUTPUTS
# ============================================================================


def pyspark_output(
    name: str,
    inputs: Union[StepResult, List[StepResult]],
    python_code: str,
    priority: int = 50,
    description: str = "",
) -> StepResult:
    """
    Define un paso de salida PySpark personalizado.

    Ejecuta código Python personalizado para escribir datos de forma customizada.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) del cual se escribirán los datos
        python_code: Código Python a ejecutar
        priority: Prioridad de ejecución
        description: Descripción del propósito de este paso

    Returns:
        StepResult del paso output
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.OUTPUT,
        class_name="PySparkOutputStep",
        class_pretty_name="PySpark",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "pythonCode": python_code,
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
    )

    pipeline.add_node(node)

    # Manejar múltiples inputs
    if isinstance(inputs, list):
        for input_step in inputs:
            edge = Edge(
                origin=input_step.node.name,
                destination=node.name,
                data_type=DataRelation.VALID_DATA,
            )
            pipeline.add_edge(edge)
    else:
        edge = Edge(
            origin=inputs.node.name,
            destination=node.name,
            data_type=DataRelation.VALID_DATA,
        )
        pipeline.add_edge(edge)

    return StepResult(node, pipeline)


# ============================================================================
# STRUCTURED FILE OUTPUTS
# ============================================================================


def delta_output(
    name: str,
    inputs: Union[StepResult, List[StepResult]],
    path: str = "",
    priority: int = 50,
    save_mode: str = "Overwrite",
    save_options: str = "",
    description: str = "",
) -> StepResult:
    """
    Define un paso de salida Delta Lake.

    Escribe datos a una tabla Delta Lake en DBFS o S3.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) del cual se escribirán los datos
        path: Ruta de destino de la tabla Delta
        priority: Prioridad de ejecución
        save_mode: Modo de guardado (Overwrite, Append, Ignore, Error)
        save_options: Opciones adicionales
        description: Descripción del propósito de este paso

    Returns:
        StepResult del paso output
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.OUTPUT,
        class_name="DeltaOutputStep",
        class_pretty_name="Delta",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "path": path,
            "saveMode": save_mode,
            "saveOptions": save_options,
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
    )

    pipeline.add_node(node)

    # Manejar múltiples inputs
    if isinstance(inputs, list):
        for input_step in inputs:
            edge = Edge(
                origin=input_step.node.name,
                destination=node.name,
                data_type=DataRelation.VALID_DATA,
            )
            pipeline.add_edge(edge)
            _attach_outputs_writer(input_step, node.name, save_mode)
    else:
        edge = Edge(
            origin=inputs.node.name,
            destination=node.name,
            data_type=DataRelation.VALID_DATA,
        )
        pipeline.add_edge(edge)
        _attach_outputs_writer(inputs, node.name, save_mode)

    return StepResult(node, pipeline)


def parquet_output(
    name: str,
    inputs: Union[StepResult, List[StepResult]],
    path: str = "",
    priority: int = 50,
    save_mode: str = "Overwrite",
    save_options: str = "",
    description: str = "",
) -> StepResult:
    """
    Define un paso de salida Parquet.

    Escribe datos a archivos Parquet.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) del cual se escribirán los datos
        path: Ruta de destino
        priority: Prioridad de ejecución
        save_mode: Modo de guardado (Overwrite, Append, Ignore, Error)
        save_options: Opciones adicionales
        description: Descripción del propósito de este paso

    Returns:
        StepResult del paso output
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.OUTPUT,
        class_name="ParquetOutputStep",
        class_pretty_name="Parquet",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "path": path,
            "saveMode": save_mode,
            "saveOptions": save_options,
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
    )

    pipeline.add_node(node)

    # Manejar múltiples inputs
    if isinstance(inputs, list):
        for input_step in inputs:
            edge = Edge(
                origin=input_step.node.name,
                destination=node.name,
                data_type=DataRelation.VALID_DATA,
            )
            pipeline.add_edge(edge)
            _attach_outputs_writer(input_step, node.name, save_mode)
    else:
        edge = Edge(
            origin=inputs.node.name,
            destination=node.name,
            data_type=DataRelation.VALID_DATA,
        )
        pipeline.add_edge(edge)
        _attach_outputs_writer(inputs, node.name, save_mode)

    return StepResult(node, pipeline)


def json_output(
    name: str,
    inputs: Union[StepResult, List[StepResult]],
    path: str = "",
    priority: int = 50,
    save_mode: str = "Overwrite",
    save_options: str = "",
    description: str = "",
) -> StepResult:
    """
    Define un paso de salida JSON.

    Escribe datos a archivos JSON.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) del cual se escribirán los datos
        path: Ruta de destino
        priority: Prioridad de ejecución
        save_mode: Modo de guardado (Overwrite, Append, Ignore, Error)
        save_options: Opciones adicionales
        description: Descripción del propósito de este paso

    Returns:
        StepResult del paso output
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.OUTPUT,
        class_name="JsonOutputStep",
        class_pretty_name="Json",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "path": path,
            "saveMode": save_mode,
            "saveOptions": save_options,
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
    )

    pipeline.add_node(node)

    # Manejar múltiples inputs
    if isinstance(inputs, list):
        for input_step in inputs:
            edge = Edge(
                origin=input_step.node.name,
                destination=node.name,
                data_type=DataRelation.VALID_DATA,
            )
            pipeline.add_edge(edge)
    else:
        edge = Edge(
            origin=inputs.node.name,
            destination=node.name,
            data_type=DataRelation.VALID_DATA,
        )
        pipeline.add_edge(edge)

    return StepResult(node, pipeline)


# ============================================================================
# UNSTRUCTURED FILE OUTPUTS
# ============================================================================


def csv_output(
    name: str,
    inputs: Union[StepResult, List[StepResult]],
    path: str = "",
    priority: int = 50,
    delimiter: str = ",",
    header: bool = False,
    infer_schema: bool = False,
    save_mode: str = "Overwrite",
    save_options: str = "",
    description: str = "",
) -> StepResult:
    """
    Define un paso de salida CSV.

    Escribe datos a archivos CSV.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) del cual se escribirán los datos
        path: Ruta de destino
        priority: Prioridad de ejecución
        delimiter: Delimitador de campos (defecto ',')
        header: Si escribir encabezados
        infer_schema: Si inferir schema desde los datos
        save_mode: Modo de guardado (Overwrite, Append, Ignore, Error)
        save_options: Opciones adicionales
        description: Descripción del propósito de este paso

    Returns:
        StepResult del paso output
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.OUTPUT,
        class_name="CsvOutputStep",
        class_pretty_name="Csv",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "path": path,
            "delimiter": delimiter,
            "header": header,
            "inferSchema": infer_schema,
            "saveMode": save_mode,
            "saveOptions": save_options,
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
    )

    pipeline.add_node(node)

    # Manejar múltiples inputs
    if isinstance(inputs, list):
        for input_step in inputs:
            edge = Edge(
                origin=input_step.node.name,
                destination=node.name,
                data_type=DataRelation.VALID_DATA,
            )
            pipeline.add_edge(edge)
    else:
        edge = Edge(
            origin=inputs.node.name,
            destination=node.name,
            data_type=DataRelation.VALID_DATA,
        )
        pipeline.add_edge(edge)

    return StepResult(node, pipeline)


def text_output(
    name: str,
    inputs: Union[StepResult, List[StepResult]],
    path: str = "",
    priority: int = 50,
    delimiter: str = ",",
    save_mode: str = "Overwrite",
    save_options: str = "",
    description: str = "",
) -> StepResult:
    """
    Define un paso de salida Text.

    Escribe datos a archivos de texto.

    Args:
        name: Nombre único del paso
        inputs: Paso(s) previo(s) del cual se escribirán los datos
        path: Ruta de destino
        priority: Prioridad de ejecución
        delimiter: Delimitador de campos
        save_mode: Modo de guardado (Overwrite, Append, Ignore, Error)
        save_options: Opciones adicionales
        description: Descripción del propósito de este paso

    Returns:
        StepResult del paso output
    """
    pipeline = get_current_pipeline()

    node = Node(
        name=name,
        step_type=StepType.OUTPUT,
        class_name="TextOutputStep",
        class_pretty_name="Text",
        execution_engine=ExecutionEngine.HYBRID,
        priority=priority,
        description=description,
        configuration={
            "path": path,
            "delimiter": delimiter,
            "saveMode": save_mode,
            "saveOptions": save_options,
            "debugOptions": {
                "executeStepAutoDebug": True,
                "executeStepDebug": True,
                "mockType": "NoMock",
            },
        },
        supported_engines=["Streaming", "Batch", "Hybrid"],
    )

    pipeline.add_node(node)

    # Manejar múltiples inputs
    if isinstance(inputs, list):
        for input_step in inputs:
            edge = Edge(
                origin=input_step.node.name,
                destination=node.name,
                data_type=DataRelation.VALID_DATA,
            )
            pipeline.add_edge(edge)
    else:
        edge = Edge(
            origin=inputs.node.name,
            destination=node.name,
            data_type=DataRelation.VALID_DATA,
        )
        pipeline.add_edge(edge)

    return StepResult(node, pipeline)
