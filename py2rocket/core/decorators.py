"""
DSL Decorators para Stratio Rocket

Define el decorator @pipeline que permite crear pipelines de forma declarativa.

El decorator:
1. Captura la definición del pipeline
2. Ejecuta la función para construir el DAG
3. Retorna el objeto Pipeline con el IR completo
"""

import os
from typing import Callable, Dict, Any, Optional, Union, List
from functools import wraps
from dotenv import load_dotenv
from py2rocket.core.pipeline import (
    Pipeline,
    ExecutionEngine,
    PythonEnvDefinition,
    GlobalSettings,
    ErrorsManagement,
    StructuredStreamingSettings,
)
from py2rocket.core.input import set_current_pipeline as _set_current_pipeline_input
from py2rocket.core.transformation import (
    set_current_pipeline as _set_current_pipeline_transform,
)
from py2rocket.core.output import set_current_pipeline as _set_current_pipeline_output

# Cargar variables de entorno del archivo .env
load_dotenv()


def _get_project_id_from_env() -> Optional[str]:
    """
    Obtiene el PROJECT_ID del archivo .env.

    Returns:
        El valor de PROJECT_ID si existe, None en caso contrario
    """
    return os.getenv("PROJECT_ID")


def pipeline(
    name: str,
    execution_engine: str = "Hybrid",
    params: Optional[Dict[str, str]] = None,
    description: str = "",
    workflow_id: Optional[str] = None,
    version: int = 0,
    project_id: Optional[str] = None,
    group_id: Optional[str] = None,
    group_name: Optional[str] = None,
    asset_id: Optional[str] = None,
    parameters_lists: Optional[list] = None,
    pre_execution_sql_sentences: Optional[list] = None,
    post_execution_sql_sentences: Optional[list] = None,
    udfs_to_register: Optional[list] = None,
    udafs_to_register: Optional[list] = None,
    user_spark_conf: Optional[Union[Dict[str, str], List[Dict[str, str]]]] = None,
    python_env_definition: Optional[Union[Dict[str, Any], PythonEnvDefinition]] = None,
    global_settings: Optional[GlobalSettings] = None,
    errors_management: Optional[ErrorsManagement] = None,
    structured_streaming_settings: Optional[StructuredStreamingSettings] = None,
    plugins: Optional[list] = None,
    raw_ui_settings: Optional[Dict[str, Any]] = None,
    raw_metadata: Optional[Dict[str, Any]] = None,
    annotations: Optional[list] = None,
    node_groups: Optional[list] = None,
    skip_validation: bool = False,
) -> Callable:
    """
    Decorator para definir un pipeline de Stratio Rocket.

    Marca una función como definición de pipeline. La función debe contener
    las operaciones (sql, pyspark, print, etc.) que componen el flujo.

    Args:
        name: Nombre único del pipeline
        execution_engine: Motor de ejecución (Batch, Streaming, Hybrid)
        params: Parámetros del pipeline con valores por defecto
        description: Descripción del propósito del pipeline
        project_id: UUID del proyecto (obtenido de la API)
        group_id: UUID del grupo (obtenido de la API)
        group_name: Nombre/path del grupo o proyecto
        asset_id: UUID del asset creado en Rocket
        parameters_lists: Listas adicionales de parámetros a incluir
        pre_execution_sql_sentences: Lista de sentencias SQL a ejecutar antes del pipeline
        post_execution_sql_sentences: Lista de sentencias SQL a ejecutar después del pipeline
        udfs_to_register: Lista de UDFs (User Defined Functions) a registrar
        udafs_to_register: Lista de UDAFs (User Defined Aggregate Functions) a registrar
        user_spark_conf: Configuraciones Spark personalizadas (dict o lista de dicts)
        python_env_definition: Configuración pythonEnvDefinition de Rocket
        global_settings: Configuración tipada para settings.global
        errors_management: Configuración tipada para settings.errorsManagement
        structured_streaming_settings: Configuración tipada para settings.structuredStreamingSettings
        plugins: Lista de nombres de plugins a incluir en el build
        raw_ui_settings: uiSettings del JSON original (opcional)
        raw_metadata: Metadatos de primer nivel del JSON original (opcional)
        annotations: Annotations del pipelineGraph (opcional)
        node_groups: NodeGroups del pipelineGraph (opcional)
        skip_validation: Omitir validación del pipeline (opcional)

    Returns:
        Función decorada que retorna un objeto Pipeline

    Example:
        >>> @pipeline(
        ...     name="pl-ventas-diarias",
        ...     execution_engine="Hybrid",
        ...     params={"P_FECHA": "2024-01-01"}
        ... )
        ... def mi_pipeline():
        ...     ventas = sql(
        ...         name="Load_Ventas",
        ...         query="SELECT * FROM ventas WHERE fecha = '{{P_FECHA}}'"
        ...     )
        ...     print_step(ventas)
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Pipeline:
            # Mapear string a enum
            engine_map = {
                "Batch": ExecutionEngine.BATCH,
                "Streaming": ExecutionEngine.STREAMING,
                "Hybrid": ExecutionEngine.HYBRID,
            }
            engine = engine_map.get(execution_engine, ExecutionEngine.HYBRID)

            # Crear pipeline
            # Si project_id no se proporciona, intenta obtener del .env
            effective_project_id = (
                project_id if project_id is not None else _get_project_id_from_env()
            )

            pipe = Pipeline(
                name=name,
                execution_engine=engine,
                parameters=params or {},
                description=description,
                workflow_id=workflow_id,
                version=version,
                project_id=effective_project_id,
                group_id=group_id,
                group_name=group_name,
                asset_id=asset_id,
                parameters_lists=parameters_lists or [],
                pre_execution_sql_sentences=pre_execution_sql_sentences or [],
                post_execution_sql_sentences=post_execution_sql_sentences or [],
                udfs_to_register=udfs_to_register or [],
                udafs_to_register=udafs_to_register or [],
                user_spark_conf=user_spark_conf or {},
                python_env_definition=python_env_definition,
                global_settings=global_settings or GlobalSettings(),
                errors_management=errors_management or ErrorsManagement(),
                structured_streaming_settings=structured_streaming_settings
                or StructuredStreamingSettings(),
                plugins=plugins or [],
                raw_ui_settings=raw_ui_settings,
                raw_metadata=raw_metadata or {},
                annotations=annotations or [],
                node_groups=node_groups or [],
            )

            # Establecer como pipeline activo en los módulos core
            _set_current_pipeline_input(pipe)
            _set_current_pipeline_transform(pipe)
            _set_current_pipeline_output(pipe)

            # Ejecutar la función para construir el DAG
            func(*args, **kwargs)

            # Validar el pipeline
            if not skip_validation:
                pipe.validate()

            return pipe

        return wrapper

    return decorator
