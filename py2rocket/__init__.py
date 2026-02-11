"""
py2rocket - DSL para generar pipelines de Stratio Rocket

Módulo principal que expone las funcionalidades de creación, construcción
y despliegue de pipelines de Stratio Rocket.

Comandos principales:
    - create: Crea un archivo .py base para el workflow
    - build: Compila el workflow a JSON de Rocket
    - push: Despliega el pipeline a Rocket vía API
    - run: Ejecuta un workflow en Rocket vía API
    - pull: Descarga un workflow desde Rocket vía API
"""

import os
import sys
import json
import requests
import urllib3
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
    "pull",
    "download",
    "from_json",
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
    workflow_id: Optional[str] = None,
    parameters_lists: Optional[list] = None,
    pre_execution_sql_sentences: Optional[list] = None,
    udfs_to_register: Optional[list] = None,
    udafs_to_register: Optional[list] = None,
    user_spark_conf: Optional[dict] = None,
    plugins: Optional[list] = None,
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
        plugins: Lista de nombres de plugins a incluir en el build

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
    workflow_id_str = repr(workflow_id) if workflow_id else "None"
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
        workflow_id=workflow_id_str,
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
                plugins=repr(plugins or []),
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
        if workflow_file is not None:
            # Usar el nombre del archivo .py como base
            output_path = Path(workflow_file).stem + ".json"
        else:
            # Usar el nombre del pipeline
            pipeline_name = pipeline_obj.name.replace("pl-", "").replace("-", "_")
            output_path = f"{pipeline_name}.json"

    output_file = Path(output_path)

    # Resolver plugins si aplica (usa el proyecto del archivo)
    project_id = getattr(pipeline_obj, "project_id", None)
    plugins = [p for p in (getattr(pipeline_obj, "plugins", []) or []) if p]
    if project_id and plugins:
        api_host = os.getenv("ROCKET_API_HOST")
        auth_cookie = os.getenv("ROCKET_AUTH_COOKIE")
        if api_host and auth_cookie:
            verify_ssl = _get_verify_ssl_from_env()
            if not verify_ssl:
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            }
            cookies = {"stratio-cookie": auth_cookie, "lang": "en"}
            try:
                response = requests.get(
                    f"{api_host}/extensions/findAllByProjectId/{project_id}",
                    headers=headers,
                    cookies=cookies,
                    verify=verify_ssl,
                    timeout=30,
                )
                response.raise_for_status()
                extensions = response.json()
                if isinstance(extensions, list):
                    name_to_id = {
                        str(item.get("name", "")): str(item.get("id", ""))
                        for item in extensions
                        if item.get("name") and item.get("id")
                    }
                    user_plugins_jars = []
                    for plugin_name in plugins:
                        plugin_id = name_to_id.get(plugin_name)
                        if plugin_id:
                            user_plugins_jars.append({"jarPath": plugin_id})
                        else:
                            print(
                                f"⚠️  Plugin no encontrado en extensiones del proyecto: {plugin_name}"
                            )
                    pipeline_obj.user_plugins_jars = user_plugins_jars
            except requests.exceptions.RequestException as e:
                print(f"⚠️  No se pudieron resolver plugins: {e}")
        else:
            print(
                "⚠️  ROCKET_API_HOST o ROCKET_AUTH_COOKIE no definidos; se omite resolución de plugins."
            )

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
        json_file: Ruta al archivo JSON del pipeline compilado (acepta .json, .py o sin extensión)
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
    # Soportar .py, .json o sin extensión (busca .json correspondiente)
    json_path = Path(json_file)

    if json_path.suffix == ".py" or json_path.suffix == "":
        # Si es .py o sin extensión, buscar el .json con el mismo nombre base
        json_path = json_path.with_suffix(".json")

    if not json_path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {json_path}")

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


def pull(
    workflow_file: str,
    rocket_url: Optional[str] = None,
    api_token: Optional[str] = None,
    output_file: Optional[str] = None,
    force_overwrite: bool = False,
    verify_ssl: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Descarga un workflow desde Stratio Rocket vía API.

    Descarga el JSON del workflow desde el servidor. Si el archivo ya existe,
    pregunta si desea reemplazarlo o guardarlo con otro nombre (_server).

    Args:
        workflow_file: Ruta al archivo .py o .json para obtener el workflow_id o id
        rocket_url: URL base de Rocket (ej: https://rocket.example.com)
        api_token: Cookie de autenticación. Si no se proporciona, usa ROCKET_AUTH_COOKIE
        output_file: Nombre del archivo de salida (opcional, default: mismo nombre que entrada)
        force_overwrite: Si es True, sobrescribe sin preguntar
        verify_ssl: Verificar certificados SSL (default: True)

    Returns:
        Diccionario con el resultado de la operación
    """
    # 1. Determinar el archivo y obtener el workflow_id
    input_path = Path(workflow_file)
    workflow_id = None

    # Si es .py o sin extensión, buscar el .json
    if input_path.suffix == ".py" or input_path.suffix == "":
        json_path = input_path.with_suffix(".json")
        if json_path.exists():
            try:
                json_data = json.loads(json_path.read_text(encoding="utf-8"))
                # Para .py buscamos workflow_id en el JSON compilado
                workflow_id = json_data.get("id")
            except json.JSONDecodeError:
                pass
    elif input_path.suffix == ".json":
        # Si es .json, leer el id directamente
        if input_path.exists():
            try:
                json_data = json.loads(input_path.read_text(encoding="utf-8"))
                workflow_id = json_data.get("id")
            except json.JSONDecodeError:
                pass
        json_path = input_path
    else:
        raise ValueError(f"Formato de archivo no soportado: {input_path.suffix}")

    if not workflow_id:
        raise ValueError(
            f"No se pudo obtener el workflow_id desde {workflow_file}. "
            "Asegúrate de que el archivo JSON existe y contiene el campo 'id'."
        )

    # 2. Validar configuración de API
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

    # 3. Descargar el workflow desde el servidor
    url = f"{rocket_url.rstrip('/')}/workflows/download/{workflow_id}"

    cookies = {"stratio-cookie": api_token, "lang": "en"}
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "py2rocket/" + __version__,
    }

    try:
        response = requests.get(
            url, headers=headers, cookies=cookies, verify=verify_ssl, timeout=30
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise ConnectionError(f"Error al descargar el workflow: {exc}") from exc

    try:
        workflow_data = response.json()
    except ValueError as exc:
        raise ValueError(f"Respuesta inválida del servidor: {exc}") from exc

    # 4. Determinar el nombre del archivo de salida
    if output_file:
        output_path = Path(output_file)
    else:
        # Usar el nombre base del archivo de entrada
        base_name = json_path.stem
        output_path = Path(f"{base_name}.json")

    # 5. Verificar si el archivo existe y manejar la lógica de sobrescritura
    final_output_path = output_path
    if output_path.exists() and not force_overwrite:
        # El archivo existe, retornar información para que el CLI maneje la interacción
        return {
            "status": "confirm_needed",
            "message": f"El archivo {output_path} ya existe",
            "workflow_data": workflow_data,
            "output_path": str(output_path),
            "workflow_id": workflow_id,
        }

    # 6. Guardar el archivo
    try:
        output_path.write_text(
            json.dumps(workflow_data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except IOError as exc:
        raise IOError(f"Error al guardar el archivo: {exc}") from exc

    return {
        "status": "success",
        "message": f"Workflow descargado exitosamente",
        "workflow_id": workflow_id,
        "output_file": str(output_path),
        "url": url,
    }


def download(
    workflow_id: str,
    rocket_url: Optional[str] = None,
    api_token: Optional[str] = None,
    force_overwrite: bool = False,
    verify_ssl: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Descarga un workflow desde Stratio Rocket por su ID.

    Descarga el JSON del workflow desde el servidor usando su ID.
    El nombre del archivo se toma del campo 'name' del workflow.
    Si el archivo ya existe, pregunta si desea reemplazarlo o guardarlo con sufijo _server.

    La URL de Rocket se obtiene automáticamente de la variable de entorno ROCKET_URL.

    Args:
        workflow_id: ID del workflow a descargar (UUID)
        rocket_url: URL de Rocket. Si no se proporciona, usa ROCKET_API_HOST o ROCKET_URL
        api_token: Cookie de autenticación. Si no se proporciona, usa ROCKET_AUTH_COOKIE
        force_overwrite: Si es True, sobrescribe sin preguntar
        verify_ssl: Verificar certificados SSL (default: True)

    Returns:
        Diccionario con el resultado de la operación:
        {
            'status': 'success' | 'confirm_needed',
            'message': str,
            'workflow_id': str,
            'output_file': str,
            ...
        }

    Example:
        >>> from py2rocket import download
        >>> result = download(
        ...     workflow_id="7133a9b4-d4fc-4390-9aa1-802d836a2874",
        ...     api_token="my-token"
        ... )
    """
    # 1. Validar parámetros requeridos
    if not workflow_id:
        raise ValueError("workflow_id es requerido")

    # 2. Obtener configuración de conexión de variables de entorno
    if rocket_url is None:
        rocket_url = os.getenv("ROCKET_API_HOST") or os.getenv("ROCKET_URL")
    if rocket_url is None:
        raise ValueError(
            "Debe configurar ROCKET_API_HOST/ROCKET_URL en variables de entorno o archivo .env"
        )

    if api_token is None:
        api_token = os.getenv("ROCKET_AUTH_COOKIE")
    if api_token is None:
        raise ValueError(
            "Debe proporcionar 'api_token' o configurar ROCKET_AUTH_COOKIE"
        )
    if verify_ssl is None:
        verify_ssl = _get_verify_ssl_from_env()

    # 3. Descargar el workflow desde el servidor
    url = f"{rocket_url.rstrip('/')}/workflows/download/{workflow_id}"

    cookies = {"stratio-cookie": api_token, "lang": "en"}
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "py2rocket/" + __version__,
    }

    try:
        response = requests.get(
            url, headers=headers, cookies=cookies, verify=verify_ssl, timeout=30
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise ConnectionError(f"Error al descargar el workflow: {exc}") from exc

    try:
        workflow_data = response.json()
    except ValueError as exc:
        raise ValueError(f"Respuesta inválida del servidor: {exc}") from exc

    # 4. Determinar el nombre del archivo desde el campo 'name' del workflow
    workflow_name = workflow_data.get("name", "workflow")
    # Sanitizar el nombre para usar como nombre de archivo
    import re

    safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", workflow_name)
    output_path = Path(f"{safe_name}.json")

    # 5. Verificar si el archivo existe y manejar la lógica de sobrescritura
    if output_path.exists() and not force_overwrite:
        # El archivo existe, retornar información para que el CLI maneje la interacción
        return {
            "status": "confirm_needed",
            "message": f"El archivo {output_path} ya existe",
            "workflow_data": workflow_data,
            "output_path": str(output_path),
            "workflow_id": workflow_id,
            "workflow_name": workflow_name,
        }

    # 6. Guardar el archivo
    try:
        output_path.write_text(
            json.dumps(workflow_data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except IOError as exc:
        raise IOError(f"Error al guardar el archivo: {exc}") from exc

    return {
        "status": "success",
        "message": f"Workflow descargado exitosamente",
        "workflow_id": workflow_id,
        "workflow_name": workflow_name,
        "output_file": str(output_path),
        "url": url,
    }


# Mapeo de className a nombre de función Python
CLASS_TO_FUNCTION = {
    # Inputs
    "SQLInputStep": ("sql", "py2rocket.core.input"),
    "CustomLiteXDInputStep": ("custom_lite_xd", "py2rocket.core.input"),
    "JdbcInputStep": ("jdbc", "py2rocket.core.input"),
    "PostgresInputStep": ("postgres", "py2rocket.core.input"),
    "PySparkInputStep": ("pyspark_input", "py2rocket.core.input"),
    "ParquetInputStep": ("parquet", "py2rocket.core.input"),
    "DeltaInputStep": ("delta", "py2rocket.core.input"),
    "JsonInputStep": ("json", "py2rocket.core.input"),
    "CsvInputStep": ("csv", "py2rocket.core.input"),
    "FileSystemInputStep": ("filesystem", "py2rocket.core.input"),
    # Transformations
    "TriggerTransformStep": ("trigger", "py2rocket.core.transformation"),
    "PySparkTransformStep": ("pyspark", "py2rocket.core.transformation"),
    "PySparkTransformerStep": ("pyspark", "py2rocket.core.transformation"),
    "AddColumnsTransformStep": ("add_columns", "py2rocket.core.transformation"),
    "DropColumnsTransformStep": ("drop_columns", "py2rocket.core.transformation"),
    "RenameColumnTransformationStep": (
        "rename_columns",
        "py2rocket.core.transformation",
    ),
    "PersistTransformStep": ("persist", "py2rocket.core.transformation"),
    "CoalesceTransformStep": ("coalesce", "py2rocket.core.transformation"),
    "RepartitionTransformStep": ("repartition", "py2rocket.core.transformation"),
    "BypassTransformStep": ("bypass", "py2rocket.core.transformation"),
    "ByPassStep": ("bypass", "py2rocket.core.transformation"),
    "FilterTransformStep": ("filter", "py2rocket.core.transformation"),
    "UnionTransformStep": ("union", "py2rocket.core.transformation"),
    "CustomLiteXDTransformStep": (
        "custom_lite_xd_transform",
        "py2rocket.core.transformation",
    ),
    # Outputs
    "PrintOutputStep": ("print_step", "py2rocket.core.output"),
    "CustomLiteXDOutputStep": ("custom_lite_xd_output", "py2rocket.core.output"),
    "JdbcOutputStep": ("jdbc_output", "py2rocket.core.output"),
    "PostgresOutputStep": ("postgres_output", "py2rocket.core.output"),
    "SFTPOutputStep": ("sftp_output", "py2rocket.core.output"),
    "PySparkOutputStep": ("pyspark_output", "py2rocket.core.output"),
    "ParquetOutputStep": ("parquet_output", "py2rocket.core.output"),
    "DeltaOutputStep": ("delta_output", "py2rocket.core.output"),
    "JsonOutputStep": ("json_output", "py2rocket.core.output"),
    "CsvOutputStep": ("csv_output", "py2rocket.core.output"),
    "TextOutputStep": ("text_output", "py2rocket.core.output"),
    "RunWorkflowOutputStep": ("run_workflow", "py2rocket.core.output"),
}


def _sanitize_var_name(name: str, imported_functions: Optional[set] = None) -> str:
    """Convierte un nombre de nodo a nombre de variable Python válido.

    Evita conflictos con nombres de funciones importadas agregando un sufijo.
    """
    # Reemplazar caracteres no válidos con underscore
    import re

    var_name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    # Asegurar que no empiece con número
    if var_name[0].isdigit():
        var_name = f"step_{var_name}"
    # Convertir a snake_case y minúsculas
    var_name = var_name.lower()

    # Si hay conflicto con funciones importadas, agregar sufijo
    if imported_functions and var_name in imported_functions:
        var_name = f"{var_name}_step"

    return var_name


def from_json(
    json_file: str,
    output_file: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Convierte un JSON de Rocket a código Python DSL.

    Lee un archivo JSON de Rocket y genera el código Python equivalente
    usando el DSL de py2rocket.

    Args:
        json_file: Ruta al archivo JSON del pipeline
        output_file: Ruta del archivo .py de salida (opcional, default: mismo nombre con .py)

    Returns:
        Diccionario con el resultado de la operación
    """
    # Valores por defecto conocidos que se deben omitir
    DEFAULT_VALUES = {
        "forceNativeQuery": False,
        "cacheTable": False,
        "asyncRefresh": False,
        "isSaved": True,
        "quoteSql": False,
        "replaceWithInputDataframe": False,
        "discardConditions": "",
        "printData": False,
        "printSchema": False,
        "printMetadata": True,
        "logLevel": "warn",
        "saveMode": "overwrite",
        "tlsEnabled": False,
        "userPassEnabled": False,
        "filterExp": "",
    }

    # 1. Leer JSON
    json_path = Path(json_file)
    if not json_path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {json_file}")

    try:
        workflow_data = json.loads(json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON inválido: {exc}") from exc

    # 2. Extraer información del pipeline
    name = workflow_data.get("name", "imported_workflow")
    execution_engine = workflow_data.get("executionEngine", "Hybrid")
    workflow_id = workflow_data.get("id")

    # Extraer parámetros desde settings
    params = {}
    settings = workflow_data.get("settings", {})
    global_settings = settings.get("global", {})
    parameters_lists = global_settings.get("parametersLists", [])
    project_id = (
        workflow_data.get("projectId")
        or workflow_data.get("project_id")
        or global_settings.get("projectId")
        or global_settings.get("project_id")
    )
    user_plugins_jars = global_settings.get("userPluginsJars", [])
    plugins = []
    if project_id and user_plugins_jars:
        api_host = os.getenv("ROCKET_API_HOST")
        auth_cookie = os.getenv("ROCKET_AUTH_COOKIE")
        if api_host and auth_cookie:
            verify_ssl = _get_verify_ssl_from_env()
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            }
            cookies = {"stratio-cookie": auth_cookie, "lang": "en"}
            try:
                response = requests.get(
                    f"{api_host}/extensions/findAllByProjectId/{project_id}",
                    headers=headers,
                    cookies=cookies,
                    verify=verify_ssl,
                    timeout=30,
                )
                response.raise_for_status()
                extensions = response.json()
                if isinstance(extensions, list):
                    id_to_name = {
                        str(item.get("id", "")): str(item.get("name", ""))
                        for item in extensions
                        if item.get("id") and item.get("name")
                    }
                    for jar in user_plugins_jars:
                        jar_id = (
                            str(jar.get("jarPath", "")) if isinstance(jar, dict) else ""
                        )
                        if not jar_id:
                            continue
                        plugin_name = id_to_name.get(jar_id)
                        if plugin_name:
                            plugins.append(plugin_name)
                        else:
                            print(
                                f"⚠️  Plugin no encontrado en extensiones del proyecto para jarPath: {jar_id}"
                            )
            except requests.exceptions.RequestException as e:
                print(f"⚠️  No se pudieron resolver plugins en from-json: {e}")
        else:
            print(
                "⚠️  ROCKET_API_HOST o ROCKET_AUTH_COOKIE no definidos; se omite resolución de plugins en from-json."
            )

    # 3. Extraer nodos y edges
    pipeline_graph = workflow_data.get("pipelineGraph", {})
    nodes = pipeline_graph.get("nodes", [])
    edges = pipeline_graph.get("edges", [])

    # 4. Clasificar y ordenar nodos
    input_nodes = sorted(
        [n for n in nodes if n.get("stepType") == "Input"], key=lambda x: x.get("name")
    )
    transform_nodes = sorted(
        [n for n in nodes if n.get("stepType") == "Transformation"],
        key=lambda x: x.get("name"),
    )
    output_nodes = sorted(
        [n for n in nodes if n.get("stepType") == "Output"], key=lambda x: x.get("name")
    )

    # 5. Crear mapa de edges (qué inputs tiene cada nodo)
    node_inputs = {}
    for edge in edges:
        dest = edge.get("destination")
        origin = edge.get("origin")
        data_type = edge.get("dataType", "ValidData")

        # Procesar ValidData (comportamiento por defecto)
        if data_type == "ValidData":
            if dest not in node_inputs:
                node_inputs[dest] = []
            node_inputs[dest].append(origin)

        # Procesar DiscardedData (datos rechazados por filtros, etc.)
        elif data_type == "DiscardedData":
            # Los datos descartados también son inputs válidos para nodos como Union
            if dest not in node_inputs:
                node_inputs[dest] = []
            node_inputs[dest].append(origin)

    # 5.5 Función de ordenamiento topológico
    def topological_sort(nodes_to_sort):
        """Ordena los nodos de forma topológica según sus dependencias"""
        sorted_nodes = []
        visited = set()
        visiting = set()

        def visit(node_name):
            if node_name in visited:
                return
            if node_name in visiting:
                # Hay un ciclo, pero en un DAG no debería pasar
                return

            visiting.add(node_name)

            # Primero visitar las dependencias
            if node_name in node_inputs:
                for dep in node_inputs[node_name]:
                    visit(dep)

            visiting.remove(node_name)
            visited.add(node_name)

            # Agregar el nodo a la lista ordenada
            for n in nodes_to_sort:
                if n.get("name") == node_name and node_name not in [
                    x.get("name") for x in sorted_nodes
                ]:
                    sorted_nodes.append(n)

        for node in nodes_to_sort:
            visit(node.get("name"))

        return sorted_nodes

    # Ordenar nodos topológicamente
    all_nodes = input_nodes + transform_nodes + output_nodes
    all_nodes = topological_sort(all_nodes)

    # Re-clasificar después del ordenamiento
    input_nodes = [n for n in all_nodes if n.get("stepType") == "Input"]
    transform_nodes = [n for n in all_nodes if n.get("stepType") == "Transformation"]
    output_nodes = [n for n in all_nodes if n.get("stepType") == "Output"]

    # 6. Generar código Python
    imports = set()
    imports.add("from py2rocket import pipeline, build")

    code_lines = []
    var_names = {}  # Map node name -> variable name

    def generate_node_code(node):
        """Genera código para un nodo"""
        node_name = node.get("name")
        class_name = node.get("className")
        config = node.get("configuration", {})

        if class_name not in CLASS_TO_FUNCTION:
            return f"    # TODO: Unsupported node type: {class_name} ({node_name})"

        try:
            func_name, module = CLASS_TO_FUNCTION[class_name]
            imports.add(f"from {module} import {func_name}")
        except Exception as e:
            print(
                f"ERROR: No se pudo procesar el nodo '{node_name}' de tipo '{class_name}'"
            )
            print(f"Detalles: {e}")
            raise

        # Obtener variable del nodo (ya debe estar en var_names)
        var_name = var_names[node_name]

        # Construir argumentos
        args = [f'name="{node_name}"']

        # Agregar configuración relevante (filtrar debugOptions y defaults)
        skip_keys = {
            "priority",
            "debugOptions",
            "genAIMetadataTableDescription",
            "genAIMetadataColumns",
            "inputSchemas",
            "genAIMetadataTablesDescription",
            "isSaved",
            "dataAsJsonEnabled",
            "inputOptions",
            "excludeGlobFilter",
            "excludeRegexFilter",
            "subdirGlobFilter",
            "subdirRegexFilter",
            "readMode",
        }

        for key, value in config.items():
            # Saltar parámetros con puntos (schema.*, etc.)
            if "." in key:
                continue
            if key in skip_keys:
                continue
            if value == "" or value == [] or value == {}:
                continue
            # Omitir valores por defecto conocidos
            if key in DEFAULT_VALUES and value == DEFAULT_VALUES[key]:
                continue

            # Convertir clave de camelCase a snake_case
            import re

            snake_key = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", key)
            snake_key = re.sub("([a-z0-9])([A-Z])", r"\1_\2", snake_key)
            snake_key = snake_key.lower()

            # Renombrar campos específicos según el tipo de función
            if func_name == "pyspark" and snake_key == "python_code":
                snake_key = "code"

            # Formatear valor
            if isinstance(value, str):
                # Detectar si es contenido multilínea (SQL, código Python, etc.)
                is_multiline = "\n" in value or len(value) > 80

                # Campos que típicamente contienen SQL o código multilínea
                multiline_fields = {
                    "query",
                    "python_code",
                    "code",
                    "sql",
                    "add_column_expression_list",
                    "trigger_sql",
                    "filterExp",
                    "filter_exp",
                    "select_expression",
                }

                if is_multiline or snake_key in multiline_fields and len(value) > 40:
                    # Usar triple comillas para contenido multilínea
                    # Escapar solo las triple comillas si existen en el contenido
                    value_escaped = value.replace('"""', r"\"\"\"")
                    args.append(f'{snake_key}="""\n{value_escaped}\n"""')
                else:
                    # Escapar comillas en el string
                    value_escaped = value.replace('"', '\\"').replace("\\", "\\\\")
                    args.append(f'{snake_key}="{value_escaped}"')
            elif isinstance(value, bool):
                args.append(f"{snake_key}={value}")
            elif isinstance(value, (int, float)):
                args.append(f"{snake_key}={value}")
            elif isinstance(value, list):
                args.append(f"{snake_key}={value}")
            elif isinstance(value, dict):
                args.append(f"{snake_key}={value}")

        # Agregar inputs si existen
        if node_name in node_inputs:
            input_vars = [var_names[inp] for inp in node_inputs[node_name]]
            if len(input_vars) == 1:
                args.append(f"inputs={input_vars[0]}")
            else:
                args.append(f"inputs=[{', '.join(input_vars)}]")

        # Agregar priority
        priority = config.get("priority", "50")
        args.append(f"priority={priority}")

        # Generar línea de código
        args_str = ",\n        ".join(args)
        node_line = f"    {var_name} = {func_name}(\n        {args_str}\n    )"

        # Procesar OutputsWriter en el nodo origen usando set_outputs_writer
        outputs_writers = node.get("outputsWriter", []) or []
        writer_lines = []
        for writer in outputs_writers:
            ow_args = []
            save_mode = writer.get("saveMode")
            table_name = writer.get("tableName")
            discard_table_name = writer.get("discardTableName")
            extra_opts = writer.get("extraOptions", {}) or {}

            check_if_empty = extra_opts.get("checkIfEmpty")
            partition_by = extra_opts.get("partitionBy")
            partition_overwrite = extra_opts.get("partitionOverwriteEnabled")
            partition_columns = extra_opts.get("partitionColumns")
            partitions = extra_opts.get("partitions")

            if save_mode:
                ow_args.append(f'save_mode="{save_mode}"')
            if table_name:
                ow_args.append(f'table_name="{table_name}"')
            if discard_table_name:
                ow_args.append(f'discard_table_name="{discard_table_name}"')
            if check_if_empty is not None:
                ow_args.append(f"check_if_empty={check_if_empty}")

            if isinstance(partition_by, str):
                parts = [p.strip() for p in partition_by.split(",") if p.strip()]
                if parts:
                    ow_args.append(f"partition_by={parts}")
            elif isinstance(partition_by, list) and partition_by:
                ow_args.append(f"partition_by={partition_by}")

            if partition_overwrite is not None:
                ow_args.append(f"partition_overwrite_enabled={partition_overwrite}")

            if isinstance(partition_columns, str):
                cols = [p.strip() for p in partition_columns.split(",") if p.strip()]
                if cols:
                    ow_args.append(f"partition_columns={cols}")
            elif isinstance(partition_columns, list) and partition_columns:
                ow_args.append(f"partition_columns={partition_columns}")

            if partitions not in (None, ""):
                if isinstance(partitions, str) and partitions.isdigit():
                    partitions = int(partitions)
                ow_args.append(f"partitions={partitions}")

            if ow_args:
                writer_lines.append(
                    f"    {var_name}.set_outputs_writer({', '.join(ow_args)})"
                )

        if writer_lines:
            return node_line + "\n" + "\n".join(writer_lines)
        return node_line

    # Pre-procesar todos los nodos para llenar var_names
    # Primero, recopilar todos los nombres de funciones que se importarán
    imported_functions = set()
    for node in input_nodes + transform_nodes + output_nodes:
        class_name = node.get("className")
        if class_name in CLASS_TO_FUNCTION:
            func_name, _ = CLASS_TO_FUNCTION[class_name]
            imported_functions.add(func_name)

    # Ahora sanitizar nombres de variables evitando conflictos
    for node in input_nodes + transform_nodes + output_nodes:
        node_name = node.get("name")
        var_name = _sanitize_var_name(node_name, imported_functions)
        var_names[node_name] = var_name

    # Generar código para cada tipo de nodo
    if input_nodes:
        code_lines.append("    # Input nodes")
        for node in input_nodes:
            code_lines.append(generate_node_code(node))
        code_lines.append("")

    if transform_nodes:
        code_lines.append("    # Transformation nodes")
        for node in transform_nodes:
            code_lines.append(generate_node_code(node))
        code_lines.append("")

    if output_nodes:
        code_lines.append("    # Output nodes")
        for node in output_nodes:
            code_lines.append(generate_node_code(node))

    # 7. Construir el archivo Python completo
    python_code = '"""\nWorkflow generado desde JSON de Rocket\n\n'
    python_code += f"Workflow: {name}\n"
    if workflow_id:
        python_code += f"ID: {workflow_id}\n"
    python_code += '"""\n\n'

    # Imports
    python_code += "\n".join(sorted(imports)) + "\n\n\n"

    # Decorator
    decorator_args = [f'name="{name}"', f'execution_engine="{execution_engine}"']
    if params:
        decorator_args.append(f"params={params}")
    if workflow_id:
        decorator_args.append(f'workflow_id="{workflow_id}"')
    if parameters_lists and parameters_lists != [
        "Environment",
        "SparkResources",
        "SparkConfigurations",
    ]:
        decorator_args.append(f"parameters_lists={parameters_lists}")
    if plugins:
        decorator_args.append(f"plugins={plugins}")

    decorator_str = ",\n    ".join(decorator_args)
    python_code += f"@pipeline(\n    {decorator_str}\n)\n"
    python_code += "def workflow():\n"
    python_code += '    """\n    Workflow importado desde JSON de Rocket.\n    """\n'

    # Body
    python_code += "\n".join(code_lines)

    # Main block
    python_code += '\n\n\nif __name__ == "__main__":\n'
    python_code += "    # Construir el pipeline\n"
    python_code += "    pipe = workflow()\n\n"
    python_code += "    # Compilar a JSON\n"
    output_json = json_path.stem + "_rebuilt.json"
    python_code += f'    build(pipe, "{output_json}")\n'

    # 8. Guardar archivo
    if output_file:
        output_path = Path(output_file)
    else:
        output_path = json_path.with_suffix(".py")

    # Limpiar líneas en blanco innecesarias y espacios al final
    lines = python_code.split("\n")
    cleaned_lines = []
    prev_blank = False

    for line in lines:
        # Eliminar espacios al final de cada línea
        line = line.rstrip()

        # Si la línea está vacía
        is_blank = not line

        # Permitir máximo una línea en blanco consecutiva
        if is_blank:
            if not prev_blank:
                cleaned_lines.append("")
            prev_blank = True
        else:
            cleaned_lines.append(line)
            prev_blank = False

    # Eliminar líneas en blanco al final del archivo
    while cleaned_lines and not cleaned_lines[-1]:
        cleaned_lines.pop()

    # Asegurar que el archivo termina con un solo newline
    cleaned_code = "\n".join(cleaned_lines) + "\n"

    try:
        output_path.write_text(cleaned_code, encoding="utf-8")
    except IOError as exc:
        raise IOError(f"Error al guardar el archivo: {exc}") from exc

    return {
        "status": "success",
        "message": f"Código Python generado exitosamente",
        "input_file": str(json_path),
        "output_file": str(output_path),
        "nodes_count": len(nodes),
        "inputs": len(input_nodes),
        "transforms": len(transform_nodes),
        "outputs": len(output_nodes),
    }
