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
import datetime as dt
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from pathlib import Path
from typing import Optional, Dict, Any, List, Union

from dotenv import load_dotenv
from py2rocket.core import pipeline, RocketCompiler
from py2rocket.core.pipeline import (
    UIPosition,
    PythonEnvDefinition,
    GlobalSettings,
    AutoDebugSettings,
    ExecutionMetricsSettings,
    ErrorsManagement,
    GenericErrorManagement,
    StructuredStreamingSettings,
)
from py2rocket.core.step_defaults import _get_step_defaults
from py2rocket.templates.workflow_template import WORKFLOW_TEMPLATE

__version__ = "0.4.7"
__all__ = [
    "create",
    "build",
    "render",
    "push",
    "create_asset",
    "create_workflow_version",
    "run",
    "pull",
    "download",
    "get_execution_history",
    "get_projects",
    "get_workflow_run_parameters",
    "from_json",
    "validate_standard",
    "pipeline",
    "UIPosition",
]

# Cargar variables de entorno del archivo .env
load_dotenv()


def _get_suppress_insecure_warning_from_env() -> bool:
    """Obtiene ROCKET_SUPPRESS_INSECURE_REQUEST_WARNING desde .env (default: True)."""
    value = os.getenv("ROCKET_SUPPRESS_INSECURE_REQUEST_WARNING")
    if value is None:
        return True
    value = value.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return True


# Suprimir globalmente advertencias SSL inseguras de requests/urllib3 (configurable por .env)
if _get_suppress_insecure_warning_from_env():
    urllib3.disable_warnings(InsecureRequestWarning)


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


def _format_request_exception(exc: requests.exceptions.RequestException) -> str:
    """Devuelve detalle de error HTTP incluyendo status_code y response.text cuando exista."""

    def _log_http_error(detail: str) -> None:
        log_file = (os.getenv("ROCKET_HTTP_ERROR_LOG_FILE") or "").strip()
        if not log_file:
            return
        try:
            log_path = Path(log_file).expanduser()
            if log_path.parent != Path(""):
                log_path.parent.mkdir(parents=True, exist_ok=True)

            max_bytes = 5 * 1024 * 1024
            if log_path.exists() and log_path.stat().st_size >= max_bytes:
                rotated_path = log_path.with_name(log_path.name + ".1")
                if rotated_path.exists():
                    rotated_path.unlink()
                log_path.replace(rotated_path)

            timestamp = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
            with log_path.open("a", encoding="utf-8") as handler:
                handler.write(f"[{timestamp}] {detail}\n")
        except OSError:
            pass

    base_message = str(exc)
    response = getattr(exc, "response", None)
    if response is None:
        _log_http_error(base_message)
        return base_message

    status_code = getattr(response, "status_code", None)
    response_text = (getattr(response, "text", "") or "").strip()
    if len(response_text) > 1000:
        response_text = response_text[:1000] + "... [truncated]"

    formatted_message = (
        f"{base_message} | status_code={status_code} | response_text={response_text}"
    )
    _log_http_error(formatted_message)
    return formatted_message


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
    post_execution_sql_sentences: Optional[list] = None,
    udfs_to_register: Optional[list] = None,
    udafs_to_register: Optional[list] = None,
    user_spark_conf: Optional[Union[Dict[str, str], List[Dict[str, str]]]] = None,
    python_env_definition: Optional[Any] = None,
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
        post_execution_sql_sentences: Lista de sentencias SQL a ejecutar después del pipeline
        udfs_to_register: Lista de UDFs (User Defined Functions) a registrar
        udafs_to_register: Lista de UDAFs (User Defined Aggregate Functions) a registrar
        user_spark_conf: Configuraciones Spark personalizadas (dict o lista de dicts)
        python_env_definition: Configuración pythonEnvDefinition (dict o PythonEnvDefinition)
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
    post_execution_sql_sentences_str = (
        repr(post_execution_sql_sentences) if post_execution_sql_sentences else "[]"
    )
    udfs_to_register_str = repr(udfs_to_register) if udfs_to_register else "[]"
    udafs_to_register_str = repr(udafs_to_register) if udafs_to_register else "[]"
    user_spark_conf_str = repr(user_spark_conf) if user_spark_conf else "{}"
    if isinstance(python_env_definition, PythonEnvDefinition):
        python_env_definition = python_env_definition.to_dict()
    python_env_definition_str = (
        repr(python_env_definition) if python_env_definition else "None"
    )
    plugins_str = repr(plugins) if plugins else "[]"

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
        post_execution_sql_sentences=post_execution_sql_sentences_str,
        udfs_to_register=udfs_to_register_str,
        udafs_to_register=udafs_to_register_str,
        user_spark_conf=user_spark_conf_str,
        python_env_definition=python_env_definition_str,
        plugins=plugins_str,
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
    format_pyspark_code: bool = False,
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
        format_pyspark_code: Si es True, formatea campos pythonCode de nodos
                    PySpark con black antes de guardar el JSON.

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
        pipeline_obj = _load_pipeline_from_workflow(workflow_file)

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
                print(
                    "⚠️  No se pudieron resolver plugins: "
                    f"{_format_request_exception(e)}"
                )
        else:
            print(
                "⚠️  ROCKET_API_HOST o ROCKET_AUTH_COOKIE no definidos; se omite resolución de plugins."
            )

    # Compilar el pipeline
    compiler = RocketCompiler(pipeline_obj)

    # Guardar JSON
    compiler.save(
        str(output_file),
        format_pyspark_code=format_pyspark_code,
    )

    print(f"[+] Pipeline compilado: {output_path}")
    print(f"  - Nombre: {pipeline_obj.name}")
    print(f"  - Nodos: {len(pipeline_obj.nodes)}")
    print(f"  - Edges: {len(pipeline_obj.edges)}")
    print(f"  - Motor: {pipeline_obj.execution_engine.value}")

    return str(output_path)


def render(
    pipeline_obj: Any = None,
    workflow_file: Optional[str] = None,
    output_path: Optional[str] = None,
    indent: int = 2,
) -> Dict[str, Any]:
    """
    Renderiza un JSON compacto del grafo (nodes/edges) para graficación.

    Args:
        pipeline_obj: Objeto Pipeline ya construido.
        workflow_file: Ruta a archivo .py o .json del workflow.
        output_path: Ruta donde guardar el JSON del grafo (opcional).
        indent: Indentación del JSON (default: 2)

    Returns:
        Diccionario con la estructura del grafo: {"nodes": [...], "edges": [...]}.
    """
    if pipeline_obj is None and workflow_file is None:
        raise ValueError("Debe proporcionar 'pipeline_obj' o 'workflow_file'")

    graph: Dict[str, Any]

    if pipeline_obj is None and workflow_file is not None:
        input_path = Path(workflow_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {workflow_file}")

        if input_path.suffix.lower() == ".json":
            graph = _graph_from_json(input_path)
        else:
            pipeline_obj = _load_pipeline_from_workflow(workflow_file)
            graph = _graph_from_pipeline(pipeline_obj)
    else:
        graph = _graph_from_pipeline(pipeline_obj)

    if output_path:
        output_file = Path(output_path)
        output_file.write_text(
            json.dumps(graph, ensure_ascii=False, indent=indent), encoding="utf-8"
        )

    return graph


def _normalize_priority_value(value: Any) -> str:
    """Normaliza prioridad para comparación de duplicados."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(int(value))
    try:
        return str(int(str(value).strip()))
    except (TypeError, ValueError):
        return str(value).strip()


def validate_standard(workflow_file: str) -> Dict[str, Any]:
    """
    Valida estándares básicos de un pipeline desde .json o .py.

    Reglas:
    - El pipeline debe tener descripción no vacía.
    - Todos los nodos deben tener descripción no vacía.
    - No deben existir prioridades repetidas entre nodos.

    Args:
        workflow_file: Ruta al archivo de entrada (.json o .py). Si no tiene
            extensión, intenta resolver primero .json y luego .py.

    Returns:
        Diccionario con resultado y detalle de hallazgos.
    """
    input_path = Path(workflow_file)

    if input_path.suffix == "":
        json_candidate = input_path.with_suffix(".json")
        py_candidate = input_path.with_suffix(".py")
        if json_candidate.exists():
            input_path = json_candidate
        elif py_candidate.exists():
            input_path = py_candidate

    if not input_path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {workflow_file}")

    input_type = input_path.suffix.lower()
    if input_type not in {".json", ".py"}:
        raise ValueError("Formato no soportado. Use archivo .json o .py")

    missing_pipeline_description = False
    nodes_without_description: List[str] = []
    priorities_to_nodes: Dict[str, List[str]] = {}

    if input_type == ".json":
        payload = json.loads(input_path.read_text(encoding="utf-8"))
        pipeline_description = payload.get("description", "")
        nodes_payload = (payload.get("pipelineGraph") or {}).get("nodes") or []

        missing_pipeline_description = not str(pipeline_description or "").strip()

        for index, node in enumerate(nodes_payload, start=1):
            node_name = node.get("name") or node.get("id") or f"node_{index}"
            node_description = node.get("description", "")
            if not str(node_description or "").strip():
                nodes_without_description.append(str(node_name))

            priority = None
            configuration = node.get("configuration")
            if isinstance(configuration, dict):
                priority = configuration.get("priority")
            if priority is None:
                priority = node.get("executionPriority")

            normalized = _normalize_priority_value(priority)
            if normalized:
                priorities_to_nodes.setdefault(normalized, []).append(str(node_name))
    else:
        pipeline_obj = _load_pipeline_from_workflow(str(input_path))
        pipeline_description = getattr(pipeline_obj, "description", "")
        nodes_payload = getattr(pipeline_obj, "nodes", []) or []

        missing_pipeline_description = not str(pipeline_description or "").strip()

        for index, node in enumerate(nodes_payload, start=1):
            node_name = getattr(node, "name", None) or f"node_{index}"
            node_description = getattr(node, "description", "")
            if not str(node_description or "").strip():
                nodes_without_description.append(str(node_name))

            normalized = _normalize_priority_value(getattr(node, "priority", None))
            if normalized:
                priorities_to_nodes.setdefault(normalized, []).append(str(node_name))

    duplicate_priorities = [
        {"priority": priority, "nodes": node_names}
        for priority, node_names in priorities_to_nodes.items()
        if len(node_names) > 1
    ]
    duplicate_priorities.sort(key=lambda item: item["priority"])

    errors: List[str] = []
    if missing_pipeline_description:
        errors.append("El pipeline no tiene descripción")
    if nodes_without_description:
        errors.append(
            "Nodos sin descripción: " + ", ".join(sorted(nodes_without_description))
        )
    if duplicate_priorities:
        details = [
            f"prioridad {item['priority']} ({', '.join(item['nodes'])})"
            for item in duplicate_priorities
        ]
        errors.append("Prioridades repetidas: " + "; ".join(details))

    return {
        "valid": len(errors) == 0,
        "input_file": str(input_path),
        "input_type": "json" if input_type == ".json" else "python",
        "checked_nodes": len(nodes_payload),
        "missing_pipeline_description": missing_pipeline_description,
        "nodes_without_description": sorted(nodes_without_description),
        "duplicate_priorities": duplicate_priorities,
        "errors": errors,
    }


def _load_pipeline_from_workflow(workflow_file: str) -> Any:
    """Carga y ejecuta un archivo .py para extraer el Pipeline decorado."""
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
    pipeline_obj = None
    for item_name in dir(module):
        item = getattr(module, item_name)
        if callable(item) and hasattr(item, "__wrapped__"):
            pipeline_obj = item()
            break

    if pipeline_obj is None:
        raise ValueError(f"No se encontró un pipeline válido en {workflow_file}")

    return pipeline_obj


def _step_type_to_graph_type(step_type: Optional[str]) -> str:
    if not step_type:
        return "unknown"
    mapping = {
        "Input": "reader",
        "Transformation": "map",
        "Output": "writer",
    }
    return mapping.get(step_type, str(step_type).lower())


def _graph_from_pipeline(pipeline_obj: Any) -> Dict[str, Any]:
    nodes = []
    for node in pipeline_obj.nodes:
        step_type = getattr(node.step_type, "value", str(node.step_type))
        priority = getattr(node, "priority", 50)
        try:
            priority_value = int(priority)
        except (TypeError, ValueError):
            priority_value = 50
        nodes.append(
            {
                "id": node.name,
                "type": _step_type_to_graph_type(step_type),
                "priority": priority_value,
            }
        )

    edges = [
        {"source": edge.origin, "target": edge.destination}
        for edge in pipeline_obj.edges
    ]

    return {"nodes": nodes, "edges": edges}


def _graph_from_json(json_path: Path) -> Dict[str, Any]:
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    pipeline_graph = payload.get("pipelineGraph", {})
    nodes_payload = pipeline_graph.get("nodes", [])
    edges_payload = pipeline_graph.get("edges", [])

    nodes = []
    for node in nodes_payload:
        node_id = node.get("name") or node.get("id")
        if not node_id:
            continue
        step_type = node.get("stepType")
        priority = None
        configuration = node.get("configuration")
        if isinstance(configuration, dict):
            priority = configuration.get("priority")
        if priority is None:
            priority = node.get("executionPriority", 50)
        try:
            priority_value = int(str(priority))
        except (TypeError, ValueError):
            priority_value = 50
        nodes.append(
            {
                "id": node_id,
                "type": _step_type_to_graph_type(step_type),
                "priority": priority_value,
            }
        )

    edges = []
    for edge in edges_payload:
        origin = edge.get("origin")
        destination = edge.get("destination")
        if not origin or not destination:
            continue
        edges.append({"source": origin, "target": destination})

    return {"nodes": nodes, "edges": edges}


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
        raise ConnectionError(
            "Error al enviar el pipeline a Rocket: " f"{_format_request_exception(exc)}"
        ) from exc

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


def create_asset(
    json_file: str,
    rocket_url: str,
    api_token: Optional[str] = None,
    project_id: Optional[str] = None,
    group_id: Optional[str] = None,
    name: Optional[str] = None,
    description: str = "",
    verify_ssl: Optional[bool] = None,
    download_after_create: bool = True,
) -> Dict[str, Any]:
    """
    Crea un nuevo asset en Stratio Rocket importando un workflow completo.

    Este comando importa un workflow JSON completo y crea un nuevo asset
    (contenedor maestro) en Rocket. El asset se crea con una versión inicial
    del workflow. Opcionalmente puede descargar el workflow creado para
    obtener los IDs generados y actualizar el archivo .py.

    Flujo:
        1. Lee el JSON del workflow compilado
        2. Envía POST /assets/import con el contenido
        3. Rocket crea el asset y la primera versión del workflow
        4. (Opcional) Descarga el workflow para obtener asset_id y workflow_id

    Args:
        json_file: Ruta al archivo JSON del workflow compilado
        rocket_url: URL base de Rocket (ej: https://rocket.example.com)
        api_token: Cookie de autenticación. Si no se proporciona,
                  usa ROCKET_AUTH_COOKIE del .env
        project_id: ID del proyecto. Si no se proporciona, usa PROJECT_ID del .env
        group_id: ID del grupo/carpeta donde crear el asset
        name: Nombre del asset. Si no se proporciona, usa el nombre del workflow
        description: Descripción del asset
        verify_ssl: Verificar certificados SSL (default: True)
        download_after_create: Si descargar el workflow después de crearlo
                              para obtener los IDs generados (default: True)

    Returns:
        Diccionario con la respuesta:
        {
            'status': 'success' | 'error',
            'asset_id': 'uuid-del-asset',
            'workflow_id': 'uuid-del-workflow-version',
            'message': 'Asset creado exitosamente',
            'workflow_data': {...}  # Si download_after_create=True
        }

    Raises:
        FileNotFoundError: Si json_file no existe
        ValueError: Si faltan parámetros requeridos
        ConnectionError: Si no se puede conectar a Rocket
        PermissionError: Si el token no tiene permisos

    Example:
        >>> from py2rocket import build, create_asset
        >>> # 1. Compilar el workflow
        >>> build(workflow(), "mi_pipeline.json")
        >>>
        >>> # 2. Crear asset en Rocket
        >>> result = create_asset(
        ...     json_file="mi_pipeline.json",
        ...     rocket_url="https://rocket.mycompany.com",
        ...     api_token="my-cookie",
        ...     project_id="196c1c2d-5dfd-4756-ba37-80aa50d0f742",
        ...     group_id="99beb8c9-32e7-465f-9081-137cea8adee6"
        ... )
        >>>
        >>> print(f"Asset ID: {result['asset_id']}")
        >>> print(f"Workflow ID: {result['workflow_id']}")

    Note:
        - Este comando crea un NUEVO asset (contenedor maestro)
        - El asset incluye la primera versión del workflow
        - Para agregar versiones a un asset existente, usa create_workflow_version()
        - Los IDs generados pueden usarse para actualizar el decorator @pipeline
    """
    # 1. Validar que el archivo JSON existe
    json_path = Path(json_file)
    if not json_path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {json_file}")

    # 2. Leer el contenido del JSON
    try:
        workflow_data = json.loads(json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON inválido en {json_file}: {exc}") from exc

    # 3. Obtener parámetros de configuración
    if api_token is None:
        api_token = os.getenv("ROCKET_AUTH_COOKIE")
    if project_id is None:
        project_id = os.getenv("PROJECT_ID")
    if not rocket_url:
        rocket_url = os.getenv("ROCKET_API_HOST", "")
    if verify_ssl is None:
        verify_ssl = _get_verify_ssl_from_env()

    # 4. Validar parámetros requeridos
    if not rocket_url:
        raise ValueError("Debe proporcionar 'rocket_url' o configurar ROCKET_API_HOST")
    if not api_token:
        raise ValueError(
            "Debe proporcionar 'api_token' o configurar ROCKET_AUTH_COOKIE"
        )
    if not project_id:
        raise ValueError("Debe proporcionar 'project_id' o configurar PROJECT_ID")
    if not group_id:
        raise ValueError("Debe proporcionar 'group_id' para crear el asset")

    # 5. Obtener nombre del asset
    if name is None:
        name = workflow_data.get("name", "workflow")

    # 6. Construir payload para /assets/import
    import_payload = {
        "content": json.dumps(workflow_data, ensure_ascii=False),
        "assetType": "SpartaWorkflow",
        "groupId": group_id,
        "projectId": project_id,
        "name": name,
        "description": description,
    }

    # 7. Enviar POST a /assets/import
    url = f"{rocket_url.rstrip('/')}/rocket/assets/import"
    cookies = {"stratio-cookie": api_token, "lang": "en"}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "py2rocket/" + __version__,
    }

    try:
        response = requests.post(
            url,
            headers=headers,
            cookies=cookies,
            json=import_payload,
            verify=verify_ssl,
            timeout=60,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise ConnectionError(
            "Error al crear el asset en Rocket: " f"{_format_request_exception(exc)}"
        ) from exc

    # 8. Procesar respuesta
    try:
        response_data = response.json()
    except ValueError:
        response_data = {"raw": response.text}

    # 9. Extraer asset_id del response
    # La respuesta puede tener diferentes estructuras según la API
    asset_id = response_data.get("id") or response_data.get("assetId")
    workflow_id = None

    # 10. Si download_after_create, descargar el workflow para obtener los IDs
    downloaded_workflow = None
    if download_after_create and asset_id:
        # Buscar las versiones del asset
        versions_url = (
            f"{rocket_url.rstrip('/')}/rocket/assets/findAllVersions/{asset_id}"
        )
        try:
            versions_response = requests.get(
                versions_url,
                headers=headers,
                cookies=cookies,
                verify=verify_ssl,
                timeout=30,
            )
            versions_response.raise_for_status()
            versions = versions_response.json()

            if versions and len(versions) > 0:
                # Tomar la primera versión (la recién creada)
                workflow_id = versions[0].get("id")

                # Descargar el workflow completo
                if workflow_id:
                    download_url = f"{rocket_url.rstrip('/')}/rocket/workflows/download/{workflow_id}"
                    download_response = requests.get(
                        download_url,
                        headers=headers,
                        cookies=cookies,
                        verify=verify_ssl,
                        timeout=30,
                    )
                    download_response.raise_for_status()
                    downloaded_workflow = download_response.json()
        except requests.exceptions.RequestException:
            # Si falla la descarga, continuar sin ella
            pass

    return {
        "status": "success",
        "asset_id": asset_id,
        "workflow_id": workflow_id,
        "message": f"Asset '{name}' creado exitosamente",
        "response": response_data,
        "workflow_data": downloaded_workflow,
    }


def create_workflow_version(
    json_file: str,
    asset_id: str,
    rocket_url: str,
    api_token: Optional[str] = None,
    comment: str = "",
    verify_ssl: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Crea una nueva versión de workflow dentro de un asset existente en Rocket.

    Este comando toma un workflow JSON compilado y lo sube como una nueva
    versión dentro de un asset (contenedor maestro) existente. Incrementa
    automáticamente el número de versión.

    Flujo:
        1. Lee el JSON del workflow compilado
        2. Obtiene la última versión del asset para incrementar el número
        3. Envía POST /workflows con workflowMasterId (asset_id)
        4. Rocket crea una nueva versión del workflow

    Args:
        json_file: Ruta al archivo JSON del workflow compilado
        asset_id: UUID del asset (workflowMasterId) donde crear la versión
        rocket_url: URL base de Rocket (ej: https://rocket.example.com)
        api_token: Cookie de autenticación. Si no se proporciona,
                  usa ROCKET_AUTH_COOKIE del .env
        comment: Comentario asociado a esta versión
        verify_ssl: Verificar certificados SSL (default: True)

    Returns:
        Diccionario con la respuesta:
        {
            'status': 'success' | 'error',
            'workflow_id': 'uuid-del-workflow-version',
            'version': 1,
            'message': 'Versión creada exitosamente',
            'response': {...}
        }

    Raises:
        FileNotFoundError: Si json_file no existe
        ValueError: Si faltan parámetros requeridos o JSON inválido
        ConnectionError: Si no se puede conectar a Rocket
        PermissionError: Si el token no tiene permisos

    Example:
        >>> from py2rocket import build, create_workflow_version
        >>> # 1. Compilar el workflow modificado
        >>> build(workflow(), "mi_pipeline_v2.json")
        >>>
        >>> # 2. Crear nueva versión en el asset existente
        >>> result = create_workflow_version(
        ...     json_file="mi_pipeline_v2.json",
        ...     asset_id="3d3d44bf-96bd-4f65-b731-44f14fecdbb9",
        ...     rocket_url="https://rocket.mycompany.com",
        ...     api_token="my-cookie",
        ...     comment="Agregada validación de datos"
        ... )
        >>>
        >>> print(f"Workflow Version ID: {result['workflow_id']}")
        >>> print(f"Version Number: {result['version']}")

    Note:
        - Este comando crea una NUEVA VERSIÓN dentro de un asset existente
        - El asset debe existir previamente (crear con create_asset() si es nuevo)
        - El número de versión se incrementa automáticamente
        - Para crear un asset nuevo, usa create_asset() en su lugar
    """
    # 1. Validar que el archivo JSON existe
    json_path = Path(json_file)
    if not json_path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {json_file}")

    # 2. Leer el contenido del JSON
    try:
        workflow_data = json.loads(json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON inválido en {json_file}: {exc}") from exc

    # 3. Obtener parámetros de configuración
    if api_token is None:
        api_token = os.getenv("ROCKET_AUTH_COOKIE")
    if not rocket_url:
        rocket_url = os.getenv("ROCKET_API_HOST", "")
    if verify_ssl is None:
        verify_ssl = _get_verify_ssl_from_env()

    # 4. Validar parámetros requeridos
    if not rocket_url:
        raise ValueError("Debe proporcionar 'rocket_url' o configurar ROCKET_API_HOST")
    if not api_token:
        raise ValueError(
            "Debe proporcionar 'api_token' o configurar ROCKET_AUTH_COOKIE"
        )
    if not asset_id:
        raise ValueError(
            "Debe proporcionar 'asset_id' (workflowMasterId del asset existente)"
        )

    # 5. Obtener la última versión del asset
    versions_url = f"{rocket_url.rstrip('/')}/rocket/assets/findAllVersions/{asset_id}"
    cookies = {"stratio-cookie": api_token, "lang": "en"}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "py2rocket/" + __version__,
    }

    try:
        versions_response = requests.get(
            versions_url,
            headers=headers,
            cookies=cookies,
            verify=verify_ssl,
            timeout=30,
        )
        versions_response.raise_for_status()
        versions = versions_response.json()
    except requests.exceptions.RequestException as exc:
        raise ConnectionError(
            "Error al obtener versiones del asset: " f"{_format_request_exception(exc)}"
        ) from exc

    # 6. Determinar el número de la nueva versión
    if versions and len(versions) > 0:
        # Obtener el máximo número de versión
        max_version = max(v.get("version", 0) for v in versions)
        new_version = max_version + 1
    else:
        new_version = 0

    # 7. Construir payload para POST /workflows
    workflow_payload = {
        "workflowMasterId": asset_id,
        "settings": workflow_data.get("settings", {}),
        "pipelineGraph": workflow_data.get("pipelineGraph", {}),
        "uiSettings": workflow_data.get("uiSettings", []),
        "version": new_version,
        "tags": workflow_data.get("tags", []),
    }

    # 8. Enviar POST a /workflows
    url = f"{rocket_url.rstrip('/')}/rocket/workflows"
    if comment:
        url += f"?comment={requests.utils.quote(comment)}"

    try:
        response = requests.post(
            url,
            headers=headers,
            cookies=cookies,
            json=workflow_payload,
            verify=verify_ssl,
            timeout=60,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise ConnectionError(
            "Error al crear la versión del workflow: "
            f"{_format_request_exception(exc)}"
        ) from exc

    # 9. Procesar respuesta
    try:
        response_data = response.json()
    except ValueError:
        response_data = {"raw": response.text}

    workflow_id = response_data.get("id") or response_data.get("workflowId")

    return {
        "status": "success",
        "workflow_id": workflow_id,
        "version": new_version,
        "asset_id": asset_id,
        "message": f"Versión {new_version} creada exitosamente",
        "response": response_data,
    }


def run(
    json_file: str,
    workflow_id: Optional[str] = None,
    project_id: Optional[str] = None,
    rocket_url: Optional[str] = None,
    api_token: Optional[str] = None,
    instance: str = "XS",
    params_lists: Optional[list] = None,
    params_lists_file: Optional[str] = None,
    extra_params_file: Optional[str] = None,
    extra_params: Optional[list] = None,
    execution_name: str = "",
    execution_description: str = "",
    execution_priority: int = 0,
    force_execution_if_available_resources: bool = False,
    retry_unsuccessful_writes: bool = False,
    max_attempts: int = 0,
    attempts_conditions: Optional[list] = None,
    extended_audit_info: bool = False,
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
        params_lists: Lista explícita de paramsLists (sobrescribe el JSON)
        params_lists_file: Ruta a JSON con lista de paramsLists
        extra_params_file: Ruta a un JSON con lista de parámetros extra
        extra_params: Lista de parámetros extra (sobrescribe extra_params_file si se provee)
        execution_name: Nombre de ejecución
        execution_description: Descripción de ejecución
        execution_priority: Prioridad de ejecución (int)
        force_execution_if_available_resources: Forzar ejecución si hay recursos disponibles
        retry_unsuccessful_writes: Reintentar escrituras fallidas
        max_attempts: Máximo de intentos
        attempts_conditions: Lista de condiciones de reintento
        extended_audit_info: Activar auditoría extendida
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

    # 2. Obtener paramsLists
    parameters_lists = None
    if params_lists is not None:
        parameters_lists = params_lists
    elif params_lists_file:
        params_path = Path(params_lists_file)
        if not params_path.exists():
            raise FileNotFoundError(
                f"Archivo de paramsLists no encontrado: {params_lists_file}"
            )
        try:
            parameters_lists = json.loads(params_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"JSON inválido en {params_lists_file}: {exc}") from exc
    else:
        parameters_lists = (
            pipeline_data.get("settings", {})
            .get("global", {})
            .get("parametersLists", [])
        )

    if not isinstance(parameters_lists, list):
        raise ValueError("paramsLists debe ser una lista de strings")

    if instance:
        parameters_lists = list(parameters_lists)
        if instance not in parameters_lists:
            parameters_lists.append(instance)

    # 3. workflow_id y project_id
    if workflow_id is None:
        workflow_id = pipeline_data.get("id")

    if project_id is None:
        project_id = os.getenv("PROJECT_ID")

    missing_required = []
    if not project_id:
        missing_required.append("project_id")
    if not workflow_id:
        missing_required.append("workflow_id")

    # 5. Validar configuración de API
    if rocket_url is None:
        rocket_url = os.getenv("ROCKET_API_HOST", "")
    if api_token is None:
        api_token = os.getenv("ROCKET_AUTH_COOKIE")

    if not rocket_url:
        missing_required.append("rocket_url")
    if not api_token:
        missing_required.append("api_token")

    if missing_required:
        raise ValueError(
            "Faltan parámetros requeridos para ejecutar: " + ", ".join(missing_required)
        )

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

    for item in extra_params:
        if not isinstance(item, dict) or "name" not in item or "value" not in item:
            raise ValueError(
                "Cada item de extraParams debe ser un dict con 'name' y 'value'"
            )

    if attempts_conditions is None:
        attempts_conditions = []
    if not isinstance(attempts_conditions, list):
        raise ValueError("attemptsConditions debe ser una lista")

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
            "name": execution_name or "",
            "description": execution_description or "",
            "executionPriority": int(execution_priority),
            "forceExecutionIfAvailableResources": bool(
                force_execution_if_available_resources
            ),
            "retryUnsuccessfulWrites": bool(retry_unsuccessful_writes),
            "maxAttempts": int(max_attempts),
            "attemptsConditions": attempts_conditions,
            "governanceSettings": {
                "qualityRuleSettings": {"extendedAuditInfo": bool(extended_audit_info)}
            },
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
        raise ConnectionError(
            "Error al ejecutar el workflow: " f"{_format_request_exception(exc)}"
        ) from exc

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
        raise ConnectionError(
            "Error al descargar el workflow: " f"{_format_request_exception(exc)}"
        ) from exc

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
        raise ConnectionError(
            "Error al descargar el workflow: " f"{_format_request_exception(exc)}"
        ) from exc

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


def get_execution_history(
    workflow_id: str,
    project_id: Optional[str] = None,
    rocket_url: Optional[str] = None,
    api_token: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    date_from: Optional[int] = None,
    date_to: Optional[int] = None,
    verify_ssl: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Obtiene el historial de ejecución de un workflow desde Stratio Rocket.

    Recupera un listado de todas las ejecuciones asociadas a un workflow específico,
    con opciones de filtrado por estado, fechas, límite de resultados, etc.

    Args:
        workflow_id: ID del workflow (UUID) para el cual obtener el historial
        project_id: ID del proyecto. Si no se proporciona, usa PROJECT_ID del .env
        rocket_url: URL de Rocket. Si no se proporciona, usa ROCKET_API_HOST o ROCKET_URL
        api_token: Cookie de autenticación. Si no se proporciona, usa ROCKET_AUTH_COOKIE
        status: Filtrar por estado ('Running', 'Failed', 'Stopped', 'Completed', etc.)
                Puede ser una lista separada por comas para múltiples estados
        limit: Número máximo de ejecuciones a devolver (default: 50)
        offset: Número de resultados a saltar para paginación (default: 0)
        date_from: Timestamp en ms de inicio del rango de fechas (opcional)
        date_to: Timestamp en ms de fin del rango de fechas (opcional)
        verify_ssl: Verificar certificados SSL (default: True)

    Returns:
        Diccionario con el historial de ejecuciones:
        {
            'status': 'success' | 'error',
            'message': str,
            'workflow_id': str,
            'total_count': int,
            'executions': [
                {
                    'id': str,
                    'executionNameDescription': {...},
                    'statuses': [...],
                    'startDate': datetime,
                    'endDate': datetime,
                    'state': str,
                    ...
                },
                ...
            ]
        }

    Example:
        >>> from py2rocket import get_execution_history
        >>> result = get_execution_history(
        ...     workflow_id="7133a9b4-d4fc-4390-9aa1-802d836a2874",
        ...     status="Completed",
        ...     limit=20
        ... )
        >>> print(json.dumps(result, indent=2, default=str))
    """
    # 1. Validar parámetros requeridos
    if not workflow_id:
        raise ValueError("workflow_id es requerido")

    # 2. Obtener configuración de conexión
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

    if project_id is None:
        project_id = os.getenv("PROJECT_ID")

    if verify_ssl is None:
        verify_ssl = _get_verify_ssl_from_env()

    # 3. Construir parámetros de query
    params = {
        "page": offset,
        "offset": limit,
        "projectId": project_id,
        "searchText": workflow_id,  # Buscar por ID del workflow
    }

    # Agregar parámetros opcionales
    if status:
        params["status"] = status
    if date_from is not None:
        params["dateFrom"] = date_from
    if date_to is not None:
        params["dateTo"] = date_to

    # 4. Realizar request a la API
    url = f"{rocket_url.rstrip('/')}/assetExecutions/search"

    cookies = {"stratio-cookie": api_token, "lang": "en"}
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "py2rocket/" + __version__,
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            cookies=cookies,
            params=params,
            verify=verify_ssl,
            timeout=30,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise ConnectionError(
            "Error al obtener historial de ejecuciones: "
            f"{_format_request_exception(exc)}"
        ) from exc

    try:
        executions_data = response.json()
    except ValueError as exc:
        raise ValueError(f"Respuesta inválida del servidor: {exc}") from exc

    # 5. Procesar respuesta
    # La respuesta puede ser un array directamente o tener estructura con metadata
    if isinstance(executions_data, list):
        executions = executions_data
        total_count = len(executions_data)
    elif isinstance(executions_data, dict):
        executions = executions_data.get("data", []) or executions_data.get(
            "executions", []
        )
        total_count = executions_data.get("totalCount", len(executions))
    else:
        executions = []
        total_count = 0

    return {
        "status": "success",
        "message": f"Historial de ejecuciones obtenido exitosamente",
        "workflow_id": workflow_id,
        "total_count": total_count,
        "executions": executions,
        "url": url,
    }


def get_projects(
    rocket_url: Optional[str] = None,
    api_token: Optional[str] = None,
    verify_ssl: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Obtiene la lista de todos los proyectos disponibles en Stratio Rocket.

    Recupera información base de los proyectos incluyendo ID, nombre,
    normalizedName (para uso en sincronización) y otros metadatos.

    Args:
        rocket_url: URL de Rocket. Si no se proporciona, usa ROCKET_API_HOST o ROCKET_URL
        api_token: Cookie de autenticación. Si no se proporciona, usa ROCKET_AUTH_COOKIE
        verify_ssl: Verificar certificados SSL (default: True from .env)

    Returns:
        Diccionario con la lista de proyectos:
        {
            'status': 'success' | 'error',
            'message': str,
            'total_count': int,
            'projects': [
                {
                    'id': str (UUID),
                    'name': str,
                    'normalizedName': str (para sync),
                    'groupId': str,
                    'description': str,
                    'creationDate': datetime,
                    'lastUpdateDate': datetime,
                    'creationUser': str,
                    ...
                },
                ...
            ]
        }

    Examples:
        >>> from py2rocket import get_projects
        >>> result = get_projects()
        >>> if result['status'] == 'success':
        ...     for proj in result['projects']:
        ...         print(f"{proj['name']} ({proj['normalizedName']})")
    """
    if verify_ssl is None:
        verify_ssl = os.getenv("ROCKET_VERIFY_SSL", "true").lower() == "true"

    if not rocket_url:
        rocket_url = os.getenv("ROCKET_API_HOST") or os.getenv("ROCKET_URL")
    if not api_token:
        api_token = os.getenv("ROCKET_AUTH_COOKIE")

    if not rocket_url or not api_token:
        return {
            "status": "error",
            "message": "Missing ROCKET_API_HOST and ROCKET_AUTH_COOKIE",
            "total_count": 0,
            "projects": [],
        }

    try:
        url = f"{rocket_url.rstrip('/')}/projects"

        headers = {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": f"py2rocket/{__version__}",
        }
        cookies = {"stratio-cookie": api_token}

        response = requests.get(
            url,
            headers=headers,
            cookies=cookies,
            verify=verify_ssl,
            timeout=30,
        )
        response.raise_for_status()
        projects = response.json() or []

    except requests.exceptions.RequestException as exc:
        return {
            "status": "error",
            "message": (
                "Error al conectar con Rocket: " f"{_format_request_exception(exc)}"
            ),
            "total_count": 0,
            "projects": [],
        }

    return {
        "status": "success",
        "message": f"Proyectos obtenidos exitosamente",
        "total_count": len(projects),
        "projects": projects,
        "url": url,
    }


def get_workflow_run_parameters(
    workflow_id: str,
    rocket_url: Optional[str] = None,
    api_token: Optional[str] = None,
    verify_ssl: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Obtiene los parámetros disponibles para ejecutar un workflow.

    Recupera información de los parámetros configurables para un workflow específico,
    incluyendo Environment, SparkConfiguration y SparkResources que pueden ser
    reutilizados para la ejecución.

    Args:
        workflow_id: ID del workflow (UUID) para el cual obtener los parámetros
        rocket_url: URL de Rocket. Si no se proporciona, usa ROCKET_API_HOST o ROCKET_URL
        api_token: Cookie de autenticación. Si no se proporciona, usa ROCKET_AUTH_COOKIE
        verify_ssl: Verificar certificados SSL (default: True from .env)

    Returns:
        Diccionario con los parámetros disponibles:
        {
            'status': 'success' | 'error',
            'message': str,
            'workflow_id': str,
            'groupsAndContexts': [
                {
                    'parameterList': {
                        'name': 'Environment' | 'SparkConfigurations' | 'SparkResources',
                        'parameters': [
                            {'name': str, 'value': str},
                            ...
                        ]
                    },
                    'contexts': [...]
                },
                ...
            ],
            'extraParams': [...]
        }

    Examples:
        >>> from py2rocket import get_workflow_run_parameters
        >>> result = get_workflow_run_parameters('ca8ca3b8-2d96-4f3a-a56c-cd9244f8150b')
        >>> if result['status'] == 'success':
        ...     for ctx in result['groupsAndContexts']:
        ...         param_list = ctx['parameterList']
        ...         print(f"Parámetros: {param_list['name']}")
    """
    if verify_ssl is None:
        verify_ssl = os.getenv("ROCKET_VERIFY_SSL", "true").lower() == "true"

    if not rocket_url:
        rocket_url = os.getenv("ROCKET_API_HOST") or os.getenv("ROCKET_URL")
    if not api_token:
        api_token = os.getenv("ROCKET_AUTH_COOKIE")

    if not rocket_url or not api_token:
        return {
            "status": "error",
            "message": "Missing ROCKET_API_HOST and ROCKET_AUTH_COOKIE",
            "workflow_id": workflow_id,
            "groupsAndContexts": [],
            "extraParams": [],
            "extraParamsWithDefault": [],
        }

    try:
        url = f"{rocket_url.rstrip('/')}/workflows/runWithParametersViewById/{workflow_id}"

        headers = {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": f"py2rocket/{__version__}",
        }
        cookies = {"stratio-cookie": api_token}

        response = requests.post(
            url,
            headers=headers,
            cookies=cookies,
            verify=verify_ssl,
            timeout=30,
        )
        response.raise_for_status()
        params_view = response.json() or {}

    except requests.exceptions.RequestException as exc:
        return {
            "status": "error",
            "message": (
                "Error al conectar con Rocket: " f"{_format_request_exception(exc)}"
            ),
            "workflow_id": workflow_id,
            "groupsAndContexts": [],
            "extraParams": [],
            "extraParamsWithDefault": [],
        }

    return {
        "status": "success",
        "message": f"Parámetros del workflow obtenidos exitosamente",
        "workflow_id": workflow_id,
        "groupsAndContexts": params_view.get("groupsAndContexts", []),
        "extraParams": params_view.get("extraParams", []),
        "extraParamsWithDefault": params_view.get("extraParamsWithDefault", []),
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
    "SFTPInputStep": ("sftp_input", "py2rocket.core.input"),
    "TestInputStep": ("test_input", "py2rocket.core.input"),
    # Transformations
    "TriggerTransformStep": ("trigger", "py2rocket.core.transformation"),
    "PySparkTransformStep": ("pyspark", "py2rocket.core.transformation"),
    "PySparkTransformerStep": ("pyspark", "py2rocket.core.transformation"),
    "AddColumnsTransformStep": ("add_columns", "py2rocket.core.transformation"),
    "DropColumnsTransformStep": ("drop_columns", "py2rocket.core.transformation"),
    "SelectTransformStep": ("select", "py2rocket.core.transformation"),
    "DistinctTransformStep": ("distinct", "py2rocket.core.transformation"),
    "DropDuplicatesTransformStep": (
        "drop_duplicates",
        "py2rocket.core.transformation",
    ),
    "RenameColumnTransformationStep": (
        "rename_columns",
        "py2rocket.core.transformation",
    ),
    "PersistTransformStep": ("persist", "py2rocket.core.transformation"),
    "CoalesceTransformStep": ("coalesce", "py2rocket.core.transformation"),
    "RepartitionTransformStep": ("repartition", "py2rocket.core.transformation"),
    "ByPassStep": ("bypass", "py2rocket.core.transformation"),
    "FilterTransformStep": ("filter", "py2rocket.core.transformation"),
    "UnionTransformStep": ("union", "py2rocket.core.transformation"),
    "MlModelTransformStep": ("ml_model", "py2rocket.core.transformation"),
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


def _to_python_string_literal(value: str) -> str:
    """Convierte un string a literal Python, preservando multilínea cuando aplica."""
    if not isinstance(value, str):
        return repr(value)
    # json.dumps produce un literal de string seguro y reversible para Python,
    # preservando escapes (\n, \\, regex, comillas, unicode) de forma transparente.
    return json.dumps(value, ensure_ascii=False)


def _to_python_multiline_string_literal(value: str) -> str:
    """Convierte un string multilínea a triple-quoted literal legible y seguro."""
    if not isinstance(value, str):
        return repr(value)

    normalized = value.replace("\r\n", "\n").replace("\r", "\n")
    if "\n" not in normalized:
        return _to_python_string_literal(normalized)

    escaped = normalized.replace("\\", "\\\\").replace('"""', '\\"""')
    return f'"""{escaped}"""'


def _sanitize_path_value(value: str) -> str:
    """Normaliza valores de path removiendo saltos de línea y espacios extremos."""
    if not isinstance(value, str):
        return value
    return value.replace("\n", "").replace("\r", "").strip()


def _auto_debug_settings_to_literal(value: AutoDebugSettings) -> str:
    default = AutoDebugSettings()
    args = []
    if value.enable_auto_debug != default.enable_auto_debug:
        args.append(f"enable_auto_debug={value.enable_auto_debug}")
    if (
        value.force_auto_debug_execution_for_all_steps
        != default.force_auto_debug_execution_for_all_steps
    ):
        args.append(
            "force_auto_debug_execution_for_all_steps="
            f"{value.force_auto_debug_execution_for_all_steps}"
        )
    if value.do_not_use_cache_data != default.do_not_use_cache_data:
        args.append(f"do_not_use_cache_data={value.do_not_use_cache_data}")
    if not args:
        return "AutoDebugSettings()"
    return f"AutoDebugSettings({', '.join(args)})"


def _execution_metrics_settings_to_literal(value: ExecutionMetricsSettings) -> str:
    default = ExecutionMetricsSettings()
    args = []
    if value.custom_metric_labels != default.custom_metric_labels:
        args.append(f"custom_metric_labels={repr(value.custom_metric_labels)}")
    if not args:
        return "ExecutionMetricsSettings()"
    return f"ExecutionMetricsSettings({', '.join(args)})"


def _global_settings_to_literal(value: GlobalSettings) -> str:
    default = GlobalSettings()
    args = []
    if value.execution_mode != default.execution_mode:
        args.append(f"execution_mode={repr(value.execution_mode)}")
    if value.enable_quality_rules != default.enable_quality_rules:
        args.append(f"enable_quality_rules={value.enable_quality_rules}")
    if value.auto_debug_settings != default.auto_debug_settings:
        args.append(
            "auto_debug_settings="
            f"{_auto_debug_settings_to_literal(value.auto_debug_settings)}"
        )
    if value.get_total_rows_by_step != default.get_total_rows_by_step:
        args.append(f"get_total_rows_by_step={value.get_total_rows_by_step}")
    if value.enable_project_env_var != default.enable_project_env_var:
        args.append(f"enable_project_env_var={value.enable_project_env_var}")
    if value.execution_metrics_settings != default.execution_metrics_settings:
        args.append(
            "execution_metrics_settings="
            f"{_execution_metrics_settings_to_literal(value.execution_metrics_settings)}"
        )
    if not args:
        return "GlobalSettings()"
    return f"GlobalSettings({', '.join(args)})"


def _errors_management_to_literal(value: ErrorsManagement) -> str:
    default = ErrorsManagement()
    args = []
    if (
        value.generic_error_management.when_error
        != default.generic_error_management.when_error
    ):
        args.append(
            "generic_error_management=GenericErrorManagement("
            f"when_error={repr(value.generic_error_management.when_error)}"
            ")"
        )
    if not args:
        return "ErrorsManagement()"
    return f"ErrorsManagement({', '.join(args)})"


def _structured_streaming_settings_to_literal(
    value: StructuredStreamingSettings,
) -> str:
    if value.settings:
        return f"StructuredStreamingSettings(settings={repr(value.settings)})"
    return "StructuredStreamingSettings()"


def _extract_group_name_from_metadata(raw_metadata: Dict[str, Any]) -> Optional[str]:
    """
    Extrae el nombre/path del grupo desde raw_metadata.

    Returns:
        El nombre del grupo si existe, None en caso contrario
    """
    if not raw_metadata:
        return None

    group = raw_metadata.get("group", {})
    if isinstance(group, dict):
        return group.get("name")

    return None


def from_json(
    json_file: str,
    output_file: Optional[str] = None,
    asset_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Convierte un JSON de Rocket a código Python DSL.

    Lee un archivo JSON de Rocket y genera el código Python equivalente
    usando el DSL de py2rocket.

    Args:
        json_file: Ruta al archivo JSON del pipeline
        output_file: Ruta del archivo .py de salida (opcional, default: mismo nombre con .py)
        asset_id: ID del asset asociado al workflow (opcional)

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
    version = workflow_data.get("version", 0)
    if not isinstance(version, int):
        try:
            version = int(version)
        except (TypeError, ValueError):
            version = 0
    # Usar el asset_id pasado como parámetro, o intentar obtenerlo del JSON
    if not asset_id:
        asset_id = workflow_data.get("workflowMasterId") or workflow_data.get("assetId")

    # Extraer parámetros desde settings
    params = {}
    settings = workflow_data.get("settings", {})
    global_settings_dict = settings.get("global", {})
    sql_settings = global_settings_dict.get("sqlSettings", {})
    global_settings_obj = GlobalSettings.from_dict(global_settings_dict)
    errors_management_obj = ErrorsManagement.from_dict(
        settings.get("errorsManagement", {})
    )
    structured_streaming_settings_obj = StructuredStreamingSettings.from_dict(
        settings.get("structuredStreamingSettings", {})
    )
    python_env_definition = settings.get("pythonEnvDefinition")
    python_env_definition_obj = None
    if isinstance(python_env_definition, dict):
        python_env_definition_obj = PythonEnvDefinition.from_dict(python_env_definition)

    def _extract_sentence_values(items):
        values = []
        if not isinstance(items, list):
            return values
        for item in items:
            if isinstance(item, dict):
                sentence = item.get("sentence")
                if sentence:
                    values.append(sentence)
            elif isinstance(item, str) and item:
                values.append(item)
        return values

    def _extract_register_values(items):
        values = []
        if not isinstance(items, list):
            return values
        for item in items:
            if isinstance(item, dict):
                name = item.get("name")
                if name:
                    values.append(name)
            elif isinstance(item, str) and item:
                values.append(item)
        return values

    pre_execution_sql_sentences = _extract_sentence_values(
        sql_settings.get("preExecutionSqlSentences", [])
    )
    post_execution_sql_sentences = _extract_sentence_values(
        sql_settings.get("postExecutionSqlSentences", [])
    )
    udfs_to_register = _extract_register_values(sql_settings.get("udfsToRegister", []))
    udafs_to_register = _extract_register_values(
        sql_settings.get("udafsToRegister", [])
    )

    parameters_lists = global_settings_dict.get("parametersLists", [])
    user_defined_params = global_settings_dict.get("parametersSettings", {}).get(
        "userDefinedParameters", []
    )
    if isinstance(user_defined_params, list):
        for entry in user_defined_params:
            if isinstance(entry, dict):
                key = entry.get("customParameterName")
                val = entry.get("customParameterValue")
                if key:
                    params[key] = val
    project_id = (
        workflow_data.get("projectId")
        or workflow_data.get("project_id")
        or global_settings_dict.get("projectId")
        or global_settings_dict.get("project_id")
    )
    group_id = workflow_data.get("groupId")

    raw_ui_settings = workflow_data.get("uiSettings")
    raw_metadata_keys = [
        "group",
        "groupId",
        "projectId",
        "versionSparta",
        "creationDate",
        "lastUpdateDate",
        "version",
        "readOnly",
        "releaseInProgress",
        "tags",
        "debugMode",
        "debugAsExecutionMaybe",
        "normalizedName",
        "isHybridStreaming",
        "workflowType",
        "workflowMasterId",
    ]
    raw_metadata = {
        k: workflow_data.get(k) for k in raw_metadata_keys if k in workflow_data
    }
    user_plugins_jars = global_settings_dict.get("userPluginsJars", [])
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
                print(
                    "⚠️  No se pudieron resolver plugins en from-json: "
                    f"{_format_request_exception(e)}"
                )
        else:
            print(
                "⚠️  ROCKET_API_HOST o ROCKET_AUTH_COOKIE no definidos; se omite resolución de plugins en from-json."
            )

    # Extraer userSparkConf si tiene valores personalizados (no vacío)
    user_spark_conf = None
    spark_settings = settings.get("sparkSettings", {})
    spark_conf = spark_settings.get("sparkConf", {})
    user_spark_conf_list = spark_conf.get("userSparkConf", [])
    if user_spark_conf_list:
        if isinstance(user_spark_conf_list, list):
            if any(
                isinstance(item, dict) and item.get("sparkConfKey")
                for item in user_spark_conf_list
            ):
                user_spark_conf = [
                    {
                        "sparkConfKey": item.get("sparkConfKey", ""),
                        "sparkConfValue": item.get("sparkConfValue", ""),
                    }
                    for item in user_spark_conf_list
                    if isinstance(item, dict) and item.get("sparkConfKey")
                ]
            else:
                user_spark_conf = {
                    item.get("key", ""): item.get("value", "")
                    for item in user_spark_conf_list
                    if isinstance(item, dict) and item.get("key")
                }
        elif isinstance(user_spark_conf_list, dict):
            user_spark_conf = user_spark_conf_list

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

    # 4.5 Extraer outputsWriter de inputs/transformations para generar OutputWriter objects
    node_outputs_writer = {}  # {node_name: [OutputWriter_dicts]}

    for node in input_nodes + transform_nodes:
        node_name = node.get("name")
        outputs_writer = node.get("outputsWriter", [])
        if outputs_writer:
            node_outputs_writer[node_name] = outputs_writer

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
            node_inputs[dest].append((origin, data_type))

        # Procesar DiscardedData (datos rechazados por filtros, etc.)
        elif data_type == "DiscardedData":
            # Los datos descartados también son inputs válidos para nodos como Union
            if dest not in node_inputs:
                node_inputs[dest] = []
            node_inputs[dest].append((origin, data_type))

    # Detectar inputs huérfanos para evitar fallos de validación al reconstruir
    origin_nodes = {edge.get("origin") for edge in edges if edge.get("origin")}
    has_orphan_inputs = any(
        n.get("stepType") == "Input" and n.get("name") not in origin_nodes
        for n in nodes
    )

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
                for dep_name, _ in node_inputs[node_name]:
                    visit(dep_name)

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
    has_sql_objects = any(
        [
            pre_execution_sql_sentences,
            post_execution_sql_sentences,
            udfs_to_register,
            udafs_to_register,
        ]
    )
    if has_sql_objects:
        imports.add("from py2rocket.core.pipeline import SqlSentence, ToRegister")
    if python_env_definition_obj is not None:
        imports.add("from py2rocket.core.pipeline import PythonEnvDefinition")

    code_lines = []
    var_names = {}  # Map node name -> variable name

    def generate_node_code(node):
        """Genera código para un nodo"""
        node_name = node.get("name")
        class_name = node.get("className")
        class_pretty_name = node.get("classPrettyName")
        config = node.get("configuration", {})

        if class_name not in CLASS_TO_FUNCTION:
            return f"    # TODO: Unsupported node type: {class_name} ({node_name})"

        try:
            func_name, module = CLASS_TO_FUNCTION[class_name]
            imports.add(f"from py2rocket.core import {func_name}")
        except Exception as e:
            print(
                f"ERROR: No se pudo procesar el nodo '{node_name}' de tipo '{class_name}'"
            )
            print(f"Detalles: {e}")
            raise

        defaults = _get_step_defaults(
            class_name=class_name,
            step_type=node.get("stepType"),
            class_pretty_name=class_pretty_name,
        )
        default_config = defaults.get("configuration", {}) if defaults else {}

        # Obtener variable del nodo (ya debe estar en var_names)
        var_name = var_names[node_name]

        # Construir argumentos
        args = [f'name="{node_name}"']

        # Detectar argumentos válidos de la función DSL
        import importlib
        import inspect

        func_obj = getattr(importlib.import_module(module), func_name)
        sig = inspect.signature(func_obj)
        valid_params = set(sig.parameters.keys())
        valid_params.discard("name")
        valid_params.discard("inputs")
        valid_params.discard("priority")
        valid_params.discard("description")

        # Config completo (defaults + overrides del JSON)
        config_args = dict(default_config) if isinstance(default_config, dict) else {}
        if isinstance(config, dict):
            config_args.update(config)

        # Config override: inicialmente tiene todo, pero se irá limpiando
        config_override = dict(config) if isinstance(config, dict) else {}
        config_override.pop("priority", None)
        # Extraer debugOptions para manejarlo por separado
        debug_options_raw = config_override.pop("debugOptions", None)

        # Extraer metadatos (isSaved, genAI*) - removerlos del config_override
        is_saved = config_override.pop("isSaved", True)  # Default es True
        gen_ai_table_desc = config_override.pop("genAIMetadataTableDescription", "")
        gen_ai_columns = config_override.pop("genAIMetadataColumns", "")
        gen_ai_tables_desc = config_override.pop("genAIMetadataTablesDescription", None)

        added_params = set()

        input_steps_with_paths = {
            "ParquetInputStep",
            "JsonInputStep",
            "CsvInputStep",
        }

        for key, value in config_args.items():
            if key == "priority":
                continue

            # Convertir clave de camelCase a snake_case
            import re

            snake_key = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", key)
            snake_key = re.sub("([a-z0-9])([A-Z])", r"\1_\2", snake_key)
            snake_key = snake_key.lower()

            alias_map = {
                "user_pass_enable": "user_pass_enabled",
            }
            if snake_key not in valid_params and snake_key in alias_map:
                snake_key = alias_map[snake_key]

            # Renombrar campos específicos según el tipo de función
            if func_name == "pyspark" and snake_key == "python_code":
                snake_key = "code"

            if snake_key in valid_params:
                if class_name == "RunWorkflowOutputStep":
                    if snake_key in {"execution_priority", "max_attempts"}:
                        if isinstance(value, str):
                            try:
                                value = int(value)
                            except (TypeError, ValueError):
                                pass
                    elif snake_key in {"variables", "contexts"}:
                        if isinstance(value, str):
                            text_value = value.strip()
                            if text_value.startswith("[") and text_value.endswith("]"):
                                try:
                                    parsed_value = json.loads(text_value)
                                    if isinstance(parsed_value, list):
                                        value = parsed_value
                                except (TypeError, ValueError, json.JSONDecodeError):
                                    pass

                if class_name in input_steps_with_paths and snake_key == "paths":
                    if not isinstance(value, list):
                        continue

                if snake_key == "path":
                    value = _sanitize_path_value(value)

                if snake_key in added_params:
                    continue
                added_params.add(snake_key)

                # Remove from config_override since it's now an explicit parameter
                config_override.pop(key, None)

                # Formatear valor
                if isinstance(value, str):
                    if class_name == "TriggerTransformStep" and snake_key == "sql":
                        args.append(
                            f"{snake_key}={_to_python_multiline_string_literal(value)}"
                        )
                    elif class_name == "SQLInputStep" and snake_key == "query":
                        args.append(
                            f"{snake_key}={_to_python_multiline_string_literal(value)}"
                        )
                    elif (
                        class_name == "FilterTransformStep"
                        and snake_key == "filter_exp"
                    ):
                        args.append(
                            f"{snake_key}={_to_python_multiline_string_literal(value)}"
                        )
                    elif class_name in {
                        "PySparkInputStep",
                        "PySparkTransformStep",
                        "PySparkTransformerStep",
                        "PySparkOutputStep",
                    } and snake_key in {"python_code", "code"}:
                        args.append(
                            f"{snake_key}={_to_python_multiline_string_literal(value)}"
                        )
                    else:
                        args.append(f"{snake_key}={_to_python_string_literal(value)}")
                elif isinstance(value, bool):
                    args.append(f"{snake_key}={value}")
                elif isinstance(value, (int, float)):
                    args.append(f"{snake_key}={value}")
                elif isinstance(value, list):
                    args.append(f"{snake_key}={value}")
                elif isinstance(value, dict):
                    args.append(f"{snake_key}={value}")
            else:
                continue

        # Agregar inputs si existen
        if node_name in node_inputs:
            input_vars = []
            for inp_name, inp_type in node_inputs[node_name]:
                base = var_names[inp_name]
                if inp_type == "DiscardedData":
                    input_vars.append(f"{base}.discarded")
                else:
                    input_vars.append(base)
            if len(input_vars) == 1:
                args.append(f"inputs={input_vars[0]}")
            else:
                args.append(f"inputs=[{', '.join(input_vars)}]")
        else:
            if (
                "inputs" in sig.parameters
                and sig.parameters["inputs"].default is inspect._empty
            ):
                args.append("inputs=[]")

        # Agregar outputs_writer si este input/transformation tiene outputsWriter configurado
        if node_name in node_outputs_writer and node.get("stepType") in {
            "Input",
            "Transformation",
        }:
            imports.add("from py2rocket.core.pipeline import OutputWriter")
            writers_list = []
            for writer in node_outputs_writer[node_name]:
                output_step_name = writer.get("outputStepName", "")
                save_mode = writer.get("saveMode", "Overwrite")
                table_name = writer.get("tableName", "")
                discard_table_name = writer.get("discardTableName", "")
                extra_options = writer.get("extraOptions", {})

                partition_by = extra_options.get("partitionBy")
                if partition_by == "overwrite":
                    partition_by = None
                partition_overwrite = extra_options.get(
                    "partitionOverwriteEnabled", True
                )
                check_if_empty = extra_options.get("checkIfEmpty", False)
                primary_key = extra_options.get("primaryKey", "")
                update_fields = extra_options.get("updateFields", "")
                partition_columns = extra_options.get("partitionColumns", "")
                partitions = extra_options.get("partitions", "")

                ow_args = [f'output_step_name="{output_step_name}"']
                if save_mode != "Overwrite":
                    ow_args.append(f'save_mode="{save_mode}"')
                if table_name:
                    ow_args.append(f'table_name="{table_name}"')
                if discard_table_name:
                    ow_args.append(f'discard_table_name="{discard_table_name}"')
                if partition_by:
                    ow_args.append(f'partition_by="{partition_by}"')
                if not partition_overwrite:
                    ow_args.append(f"partition_overwrite={partition_overwrite}")
                if check_if_empty:
                    ow_args.append(f"check_if_empty={check_if_empty}")
                if primary_key:
                    ow_args.append(f'primary_key="{primary_key}"')
                if update_fields:
                    ow_args.append(f'update_fields="{update_fields}"')
                if partition_columns:
                    ow_args.append(f'partition_columns="{partition_columns}"')
                if partitions:
                    ow_args.append(f'partitions="{partitions}"')

                writers_list.append(f"OutputWriter({', '.join(ow_args)})")

            if writers_list:
                args.append(f"outputs_writer=[{', '.join(writers_list)}]")

        # Agregar description
        if "description" in sig.parameters:
            args.append(f"description={repr(node.get('description', ''))}")

        # Agregar priority
        priority = config_args.get("priority", 50)
        try:
            priority_int = int(str(priority))
        except Exception:
            priority_int = 50
        default_priority = (
            default_config.get("priority") if isinstance(default_config, dict) else None
        )
        try:
            default_priority_int = (
                int(str(default_priority)) if default_priority is not None else None
            )
        except Exception:
            default_priority_int = None
        if "priority" in sig.parameters and (
            default_priority_int is None or priority_int != default_priority_int
        ):
            args.append(f"priority={priority_int}")

        # UI Position (extract clean x, y coordinates as integers)
        ui_config = node.get("uiConfiguration")
        if ui_config and "position" in ui_config:
            pos = ui_config["position"]
            if "x" in pos and "y" in pos:
                # Redondear coordenadas a enteros
                x_int = round(pos["x"])
                y_int = round(pos["y"])
                args.append(f"ui_position=UIPosition(x={x_int}, y={y_int})")

        # Include flags: Solo agregar cuando tienen valor NO-DEFAULT (False)
        # Defaults: include_debug_options=True, include_supported_data_relations=True, include_description=True

        # include_supported_data_relations: solo agregar si False (no tiene supportedDataRelations)
        if "supportedDataRelations" not in node:
            args.append("include_supported_data_relations=False")

        # include_description: solo agregar si False (no tiene description)
        if "description" not in node:
            args.append("include_description=False")

        # include_debug_options: solo agregar si False (no tiene debugOptions)
        if debug_options_raw is None:
            args.append("include_debug_options=False")

        # Generar línea de código
        args_str = ",\n        ".join(args)
        node_line = f"    {var_name} = {func_name}(\n        {args_str}\n    )"

        return node_line

    # Pre-procesar todos los nodos para llenar var_names
    imported_functions = set()
    for node in input_nodes + transform_nodes + output_nodes:
        class_name = node.get("className")
        if class_name in CLASS_TO_FUNCTION:
            func_name, _ = CLASS_TO_FUNCTION[class_name]
            imported_functions.add(func_name)

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

    # Check if any node has UI position to add UIPosition import
    has_ui_positions = any(
        node.get("uiConfiguration", {}).get("position") is not None for node in nodes
    )
    if has_ui_positions:
        imports.add("from py2rocket.core.pipeline import UIPosition")

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
    decorator_args.append(f"version={version}")
    if params:
        decorator_args.append(f"params={params}")
    if workflow_data.get("description"):
        decorator_args.append(f"description={repr(workflow_data.get('description'))}")
    if workflow_id:
        decorator_args.append(f'workflow_id="{workflow_id}"')
    if project_id:
        decorator_args.append(f"project_id={repr(project_id)}")
    if group_id:
        decorator_args.append(f"group_id={repr(group_id)}")
    if asset_id:
        decorator_args.append(f'asset_id="{asset_id}"')
    if parameters_lists is not None:
        decorator_args.append(f"parameters_lists={parameters_lists}")
    if pre_execution_sql_sentences:
        sentences = ", ".join(
            [
                f"SqlSentence(sentence={repr(item)})"
                for item in pre_execution_sql_sentences
            ]
        )
        decorator_args.append(f"pre_execution_sql_sentences=[{sentences}]")
    if post_execution_sql_sentences:
        sentences = ", ".join(
            [
                f"SqlSentence(sentence={repr(item)})"
                for item in post_execution_sql_sentences
            ]
        )
        decorator_args.append(f"post_execution_sql_sentences=[{sentences}]")
    if udfs_to_register:
        items = ", ".join(
            [f"ToRegister(name={repr(item)})" for item in udfs_to_register]
        )
        decorator_args.append(f"udfs_to_register=[{items}]")
    if udafs_to_register:
        items = ", ".join(
            [f"ToRegister(name={repr(item)})" for item in udafs_to_register]
        )
        decorator_args.append(f"udafs_to_register=[{items}]")
    if plugins:
        decorator_args.append(f"plugins={plugins}")
    if user_spark_conf:
        decorator_args.append(f"user_spark_conf={user_spark_conf}")
    if python_env_definition_obj is not None:
        conda_yaml_literal = _to_python_string_literal(
            python_env_definition_obj.conda_yaml_definition
        )
        decorator_args.append(
            "python_env_definition=PythonEnvDefinition("
            f"v_env_management_mode={repr(python_env_definition_obj.v_env_management_mode)}, "
            f"conda_yaml_definition={conda_yaml_literal}, "
            f"freeze_after_debug={python_env_definition_obj.freeze_after_debug}, "
            f"conda_pack_extension={repr(python_env_definition_obj.conda_pack_extension)}, "
            "execute_conda_unpack_after_activate="
            f"{python_env_definition_obj.execute_conda_unpack_after_activate}, "
            f"py_spark_native_extensions={repr(python_env_definition_obj.py_spark_native_extensions)}"
            ")"
        )

    global_settings_literal = _global_settings_to_literal(global_settings_obj)
    if global_settings_literal != "GlobalSettings()":
        decorator_args.append(f"global_settings={global_settings_literal}")
        python_code += (
            "from py2rocket.core.pipeline import "
            "GlobalSettings, AutoDebugSettings, ExecutionMetricsSettings\n\n"
        )
    errors_management_literal = _errors_management_to_literal(errors_management_obj)
    if errors_management_literal != "ErrorsManagement()":
        decorator_args.append(f"errors_management={errors_management_literal}")
        python_code += (
            "from py2rocket.core.pipeline import "
            "ErrorsManagement, GenericErrorManagement\n\n"
        )
    structured_streaming_settings_literal = _structured_streaming_settings_to_literal(
        structured_streaming_settings_obj
    )
    if structured_streaming_settings_literal != "StructuredStreamingSettings()":
        decorator_args.append(
            "structured_streaming_settings=" f"{structured_streaming_settings_literal}"
        )
        python_code += (
            "from py2rocket.core.pipeline import StructuredStreamingSettings\n\n"
        )

    # Extraer group_name de raw_metadata y añadirlo como parámetro
    if raw_metadata:
        group_name = _extract_group_name_from_metadata(raw_metadata)
        if group_name:
            decorator_args.append(f"group_name={repr(group_name)}")
    if raw_ui_settings is not None:
        decorator_args.append(f"ui_settings={repr(raw_ui_settings)}")
    # raw_metadata no se incluye ya que group_name y group_id están como parámetros separados
    # Solo incluir annotations si no está vacío
    annotations_value = pipeline_graph.get("annotations", [])
    if annotations_value:
        decorator_args.append(f"annotations={annotations_value}")
    # Solo incluir node_groups si no está vacío
    node_groups_value = pipeline_graph.get("nodeGroups", [])
    if node_groups_value:
        decorator_args.append(f"node_groups={node_groups_value}")
    # raw_nodes_order and raw_edges_order omitted - order doesn't matter, only content
    if has_orphan_inputs:
        decorator_args.append("skip_validation=True")

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

    # Formatear con black para mejorar legibilidad del archivo generado
    formatted_code = cleaned_code
    try:
        import importlib

        black = importlib.import_module("black")
        formatted_code = black.format_str(cleaned_code, mode=black.Mode())
    except (ImportError, ValueError, SyntaxError, AttributeError):
        formatted_code = cleaned_code

    try:
        output_path.write_text(formatted_code, encoding="utf-8")
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
