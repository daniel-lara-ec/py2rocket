"""
py2rocket - DSL para generar pipelines de Stratio Rocket

Módulo principal que expone las funcionalidades de creación, construcción
y despliegue de pipelines de Stratio Rocket.

Comandos principales:
    - create: Crea un archivo .py base para el workflow
    - build: Compila el workflow a JSON de Rocket
    - push: Despliega el pipeline a Rocket vía API
    - run: Ejecuta un workflow en Rocket vía API
"""

import os
import sys
import json
import requests
from pathlib import Path
from typing import Optional, Dict, Any

from dotenv import load_dotenv
from py2rocket.core import pipeline, RocketCompiler
from py2rocket.templates.workflow_template import WORKFLOW_TEMPLATE

__version__ = "0.1.0"
__all__ = [
    "create",
    "build",
    "push",
    "run",
    "pipeline",
]

# Cargar variables de entorno del archivo .env
load_dotenv()


def _get_project_id_from_env() -> Optional[str]:
    """
    Obtiene el PROJECT_ID del archivo .env.

    Returns:
        El valor de PROJECT_ID si existe, None en caso contrario
    """
    return os.getenv("PROJECT_ID")


def _get_verify_ssl_from_env() -> bool:
    """Obtiene ROCKET_VERIFY_SSL desde .env (default: True)."""
    value = os.getenv("ROCKET_VERIFY_SSL")
    if value is None:
        return True
    value = value.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return True


def create(
    name: str,
    output_path: Optional[str] = None,
    execution_engine: str = "Hybrid",
    params: Optional[Dict[str, str]] = None,
    description: str = "",
    project_id: Optional[str] = None,
    group_id: Optional[str] = None,
    asset_id: Optional[str] = None,
    parameters_lists: Optional[list] = None,
    pre_execution_sql_sentences: Optional[list] = None,
    udfs_to_register: Optional[list] = None,
    udafs_to_register: Optional[list] = None,
    user_spark_conf: Optional[dict] = None,
) -> str:
    """
    Crea un archivo .py base para un nuevo workflow de Rocket.

    Genera un archivo Python con la estructura básica de un pipeline,
    incluyendo el decorator @pipeline y ejemplos de operaciones.

    Args:
        name: Nombre del pipeline a crear
        output_path: Ruta donde crear el archivo. Si no se especifica, usa '{name}.py'
        execution_engine: Motor de ejecución (Batch, Streaming, Hybrid)
        params: Diccionario de parámetros del pipeline con valores por defecto
        description: Descripción del propósito del pipeline
        project_id: UUID del proyecto obtenido de la API. Si no se especifica, intenta obtener de PROJECT_ID en .env
        group_id: UUID del grupo obtenido de la API
        asset_id: UUID del asset creado en Rocket
        parameters_lists: Listas adicionales de parámetros a incluir
        pre_execution_sql_sentences: Lista de sentencias SQL a ejecutar antes del pipeline
        udfs_to_register: Lista de UDFs (User Defined Functions) a registrar
        udafs_to_register: Lista de UDAFs (User Defined Aggregate Functions) a registrar
        user_spark_conf: Diccionario de configuraciones Spark personalizadas

    Returns:
        Ruta del archivo creado

    Raises:
        FileExistsError: Si el archivo ya existe

    Example:
        >>> from py2rocket import create
        >>> create(
        ...     name="pl-ventas-diarias",
        ...     params={"P_FECHA": "2024-01-01"},
        ...     description="Pipeline de procesamiento de ventas diarias"
        ... )
        'pl-ventas-diarias.py'
    """
    # Si project_id no se proporciona, intenta obtener del .env
    if project_id is None:
        project_id = _get_project_id_from_env()
    # Determinar ruta de salida
    if output_path is None:
        output_path = f"{name}.py"

    output_file = Path(output_path)

    # Verificar que no exista
    if output_file.exists():
        raise FileExistsError(f"El archivo '{output_path}' ya existe")

    # Preparar parámetros para la plantilla
    params_str = repr(params) if params else "{}"
    output_json = name.replace("pl-", "").replace("-", "_") + ".json"
    project_id_str = repr(project_id) if project_id else "None"
    group_id_str = repr(group_id) if group_id else "None"
    asset_id_str = repr(asset_id) if asset_id else "None"

    # Generar contenido desde plantilla
    parameters_lists_str = repr(parameters_lists) if parameters_lists else "[]"
    pre_execution_sql_sentences_str = (
        repr(pre_execution_sql_sentences) if pre_execution_sql_sentences else "[]"
    )
    udfs_to_register_str = repr(udfs_to_register) if udfs_to_register else "[]"
    udafs_to_register_str = repr(udafs_to_register) if udafs_to_register else "[]"
    user_spark_conf_str = repr(user_spark_conf) if user_spark_conf else "{}"

    content = WORKFLOW_TEMPLATE.format(
        name=name,
        engine=execution_engine,
        params=params_str,
        description=description or f"Pipeline {name}",
        output_file=output_json,
        project_id=project_id_str,
        group_id=group_id_str,
        asset_id=asset_id_str,
        parameters_lists=parameters_lists_str,
        pre_execution_sql_sentences=pre_execution_sql_sentences_str,
        udfs_to_register=udfs_to_register_str,
        udafs_to_register=udafs_to_register_str,
        user_spark_conf=user_spark_conf_str,
    )

    # Crear directorio si no existe
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Escribir archivo
    output_file.write_text(content, encoding="utf-8")

    print(f"[+] Workflow creado: {output_path}")
    print(f"  Edita el archivo y define tu pipeline en la función workflow()")

    return str(output_path)


def build(
    pipeline_obj: Any = None,
    output_path: Optional[str] = None,
    workflow_file: Optional[str] = None,
    indent: int = 2,
) -> str:
    """
    Compila un workflow Python a JSON de Rocket.

    Toma un objeto Pipeline (o carga uno desde un archivo .py) y lo compila
    al formato JSON completo que Stratio Rocket necesita para ejecutar el workflow.

    Args:
        pipeline_obj: Objeto Pipeline a compilar. Si no se proporciona, se debe
                     especificar workflow_file
        output_path: Ruta donde guardar el JSON. Si no se especifica, usa el nombre
                    del pipeline con extensión .json
        workflow_file: Ruta a un archivo .py que contiene el workflow. Se ejecutará
                      y se extraerá el pipeline
        indent: Nivel de indentación del JSON (default: 2)

    Returns:
        Ruta del archivo JSON generado

    Raises:
        ValueError: Si no se proporciona ni pipeline_obj ni workflow_file
        FileNotFoundError: Si workflow_file no existe

    Example:
        >>> from py2rocket import pipeline, sql, print_step, build
        >>>
        >>> @pipeline(name="mi-pipeline")
        >>> def mi_workflow():
        ...     tabla = sql(name="Load", query="SELECT * FROM tabla")
        ...     print_step(tabla)
        >>>
        >>> pipe = mi_workflow()
        >>> build(pipe, "mi_pipeline.json")
        'mi_pipeline.json'

    Example (desde archivo):
        >>> build(workflow_file="mi_workflow.py", output_path="output.json")
        'output.json'
    """
    # Validar inputs
    if pipeline_obj is None and workflow_file is None:
        raise ValueError("Debe proporcionar 'pipeline_obj' o 'workflow_file'")

    # Cargar pipeline desde archivo si es necesario
    if workflow_file is not None:
        workflow_path = Path(workflow_file)
        if not workflow_path.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {workflow_file}")

        # Ejecutar el archivo para obtener el pipeline
        import importlib.util

        spec = importlib.util.spec_from_file_location("workflow_module", workflow_path)
        if spec is None or spec.loader is None:
            raise ValueError(f"No se pudo cargar el módulo: {workflow_file}")

        module = importlib.util.module_from_spec(spec)
        sys.modules["workflow_module"] = module
        spec.loader.exec_module(module)

        # Buscar la función de workflow y ejecutarla
        for item_name in dir(module):
            item = getattr(module, item_name)
            if callable(item) and hasattr(item, "__wrapped__"):
                # Es una función decorada, ejecutarla para obtener el pipeline
                pipeline_obj = item()
                break

        if pipeline_obj is None:
            raise ValueError(f"No se encontró un pipeline válido en {workflow_file}")

    # Determinar ruta de salida
    if output_path is None:
        pipeline_name = pipeline_obj.name.replace("pl-", "").replace("-", "_")
        output_path = f"{pipeline_name}.json"

    output_file = Path(output_path)

    # Compilar el pipeline
    compiler = RocketCompiler(pipeline_obj)

    # Guardar JSON
    compiler.save(str(output_file))

    print(f"[+] Pipeline compilado: {output_path}")
    print(f"  - Nombre: {pipeline_obj.name}")
    print(f"  - Nodos: {len(pipeline_obj.nodes)}")
    print(f"  - Edges: {len(pipeline_obj.edges)}")
    print(f"  - Motor: {pipeline_obj.execution_engine.value}")

    return str(output_path)


def push(
    json_file: str,
    rocket_url: str,
    api_token: Optional[str] = None,
    project_id: Optional[str] = None,
    group_id: Optional[str] = None,
    verify_ssl: Optional[bool] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Despliega un pipeline a Stratio Rocket vía API.

    Lee un archivo JSON de pipeline y lo sube a Rocket usando la API REST.
    Permite crear nuevos pipelines o actualizar existentes.

    Args:
        json_file: Ruta al archivo JSON del pipeline a desplegar
        rocket_url: URL base de Rocket (ej: https://rocket.example.com)
        api_token: Cookie de autenticación de Rocket. Si no se proporciona,
                  se buscará en la variable de entorno ROCKET_AUTH_COOKIE
        project_id: ID del proyecto en Rocket donde crear el pipeline
        group_id: ID del grupo/carpeta en Rocket
        verify_ssl: Verificar certificados SSL (default: True)
        dry_run: Si es True, valida pero no despliega (default: False)

    Returns:
        Diccionario con la respuesta de la API:
        {
            'status': 'success' | 'error',
            'pipeline_id': 'uuid-del-pipeline',
            'message': 'Pipeline desplegado exitosamente',
            'url': 'https://rocket.../pipeline/...'
        }

    Raises:
        FileNotFoundError: Si json_file no existe
        ValueError: Si faltan parámetros requeridos
        ConnectionError: Si no se puede conectar a Rocket
        PermissionError: Si el token no tiene permisos

    Example:
        >>> from py2rocket import push
        >>> result = push(
        ...     json_file="mi_pipeline.json",
        ...     rocket_url="https://rocket.mycompany.com",
        ...     api_token="my-secret-token",
        ...     project_id="196c1c2d-5dfd-4756-ba37-80aa50d0f742"
        ... )
        >>> print(result['status'])
        'success'

    Example (con variables de entorno):
        >>> import os
        >>> os.environ['ROCKET_AUTH_COOKIE'] = 'my-cookie'
        >>> os.environ['ROCKET_PROJECT_ID'] = 'project-id'
        >>>
        >>> push(
        ...     json_file="mi_pipeline.json",
        ...     rocket_url="https://rocket.mycompany.com"
        ... )

    Note:
        Esta función aún no está implementada. Se requiere:
        - Integración con la API REST de Stratio Rocket
        - Manejo de autenticación y autorización
        - Validación de JSON antes del envío
        - Manejo de errores y reintentos
        - Logging de operaciones
    """
    # 1. Validar que el archivo JSON existe
    json_path = Path(json_file)
    if not json_path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {json_file}")

    # 2. Leer el contenido del JSON
    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON inválido en {json_file}: {exc}") from exc

    # 3. Obtener cookie de autenticación (parámetro o variable de entorno)
    if api_token is None:
        api_token = os.getenv("ROCKET_AUTH_COOKIE")

    # 4. Validar parámetros requeridos
    if not rocket_url:
        rocket_url = os.getenv("ROCKET_API_HOST", "")
    if not rocket_url:
        raise ValueError("Debe proporcionar 'rocket_url' o configurar ROCKET_API_HOST")
    if not api_token:
        raise ValueError(
            "Debe proporcionar 'api_token' o configurar ROCKET_AUTH_COOKIE"
        )
    if verify_ssl is None:
        verify_ssl = _get_verify_ssl_from_env()

    # 5. Construir request HTTP a la API de Rocket
    url = f"{rocket_url.rstrip('/')}/workflows"

    # Cookies de autenticación
    cookies = {"stratio-cookie": api_token, "lang": "en"}

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    if dry_run:
        return {
            "status": "success",
            "pipeline_id": None,
            "message": "Dry run: pipeline validado, no se envió a Rocket",
            "url": url,
        }

    # 6. Enviar PUT a /workflows
    try:
        response = requests.put(
            url,
            headers=headers,
            cookies=cookies,
            json=payload,
            verify=verify_ssl,
            timeout=60,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise ConnectionError(f"Error al enviar el pipeline a Rocket: {exc}") from exc

    # 7. Procesar respuesta
    try:
        response_data = response.json()
    except ValueError:
        response_data = {"raw": response.text}

    pipeline_id = response_data.get("id") or response_data.get("workflowId")

    return {
        "status": "success",
        "pipeline_id": pipeline_id,
        "message": "Pipeline desplegado exitosamente",
        "url": url,
        "response": response_data,
    }


def run(
    json_file: str,
    workflow_id: Optional[str] = None,
    project_id: Optional[str] = None,
    rocket_url: Optional[str] = None,
    api_token: Optional[str] = None,
    instance: str = "XS",
    extra_params_file: Optional[str] = None,
    extra_params: Optional[list] = None,
    verify_ssl: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Ejecuta un workflow en Stratio Rocket vía API.

    Construye el payload de ejecución tomando parametersLists desde el JSON
    compilado y agregando el parámetro instance.

    Args:
        json_file: Ruta al archivo JSON del pipeline compilado
        workflow_id: ID del workflow a ejecutar. Si no se proporciona, se usa el "id" del JSON
        project_id: ID del proyecto en Rocket. Si no se proporciona, intenta usar PROJECT_ID del .env
        rocket_url: URL base de Rocket (ej: https://rocket.example.com)
        api_token: Cookie de autenticación. Si no se proporciona, usa ROCKET_AUTH_COOKIE
        instance: Instancia de ejecución a añadir a paramsLists (default: XS)
        extra_params_file: Ruta a un JSON con lista de parámetros extra
        extra_params: Lista de parámetros extra (sobrescribe extra_params_file si se provee)
        verify_ssl: Verificar certificados SSL (default: True)

    Returns:
        Diccionario con la respuesta de la API
    """
    # 1. Leer JSON del pipeline
    json_path = Path(json_file)
    if not json_path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {json_file}")

    try:
        pipeline_data = json.loads(json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON inválido en {json_file}: {exc}") from exc

    # 2. Obtener parametersLists del JSON
    parameters_lists = (
        pipeline_data.get("settings", {}).get("global", {}).get("parametersLists", [])
    )
    if not isinstance(parameters_lists, list):
        parameters_lists = []

    if instance:
        parameters_lists = list(parameters_lists)
        if instance not in parameters_lists:
            parameters_lists.append(instance)

    # 3. workflow_id y project_id
    if workflow_id is None:
        workflow_id = pipeline_data.get("id")

    if project_id is None:
        project_id = os.getenv("PROJECT_ID")

    if not project_id:
        raise ValueError("Debe proporcionar 'project_id' o configurar PROJECT_ID")
    if not workflow_id:
        raise ValueError("Debe proporcionar 'workflow_id' o que exista 'id' en el JSON")

    # 4. extra_params
    if extra_params is None:
        if extra_params_file:
            extra_path = Path(extra_params_file)
            if not extra_path.exists():
                raise FileNotFoundError(
                    f"Archivo de extraParams no encontrado: {extra_params_file}"
                )
            try:
                extra_params = json.loads(extra_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"JSON inválido en {extra_params_file}: {exc}"
                ) from exc
        else:
            extra_params = []

    if not isinstance(extra_params, list):
        raise ValueError("extraParams debe ser una lista de diccionarios")

    # 5. Validar configuración de API
    if rocket_url is None:
        rocket_url = os.getenv("ROCKET_API_HOST", "")
    if api_token is None:
        api_token = os.getenv("ROCKET_AUTH_COOKIE")

    if not rocket_url:
        raise ValueError("Debe proporcionar 'rocket_url' o configurar ROCKET_API_HOST")
    if not api_token:
        raise ValueError(
            "Debe proporcionar 'api_token' o configurar ROCKET_AUTH_COOKIE"
        )
    if verify_ssl is None:
        verify_ssl = _get_verify_ssl_from_env()

    url = f"{rocket_url.rstrip('/')}/workflows/runWithExecutionContext"

    cookies = {"stratio-cookie": api_token, "lang": "en"}
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    payload = {
        "projectId": project_id,
        "workflowId": workflow_id,
        "executionContext": {
            "paramsLists": parameters_lists,
            "extraParams": extra_params,
        },
        "executionSettings": {
            "name": "",
            "description": "",
            "executionPriority": 0,
            "forceExecutionIfAvailableResources": False,
            "retryUnsuccessfulWrites": False,
            "maxAttempts": 0,
            "attemptsConditions": [],
            "governanceSettings": {"qualityRuleSettings": {"extendedAuditInfo": False}},
        },
    }

    try:
        response = requests.post(
            url,
            headers=headers,
            cookies=cookies,
            json=payload,
            verify=verify_ssl,
            timeout=60,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise ConnectionError(f"Error al ejecutar el workflow: {exc}") from exc

    try:
        response_data = response.json()
    except ValueError:
        response_data = {"raw": response.text}

    return {
        "status": "success",
        "message": "Workflow ejecutado exitosamente",
        "url": url,
        "response": response_data,
    }
