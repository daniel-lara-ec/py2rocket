"""
py2rocket - DSL para generar pipelines de Stratio Rocket

Módulo principal que expone las funcionalidades de creación, construcción
y despliegue de pipelines de Stratio Rocket.

Comandos principales:
    - create: Crea un archivo .py base para el workflow
    - build: Compila el workflow a JSON de Rocket
    - push: Despliega el pipeline a Rocket vía API
"""

import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any

from py2rocket.core import pipeline, sql, pyspark, print_step, RocketCompiler
from py2rocket.templates.workflow_template import WORKFLOW_TEMPLATE

__version__ = "0.1.0"
__all__ = ["create", "build", "push", "pipeline", "sql", "pyspark", "print_step"]


def create(
    name: str,
    output_path: Optional[str] = None,
    execution_engine: str = "Hybrid",
    params: Optional[Dict[str, str]] = None,
    description: str = "",
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
    
    # Generar contenido desde plantilla
    content = WORKFLOW_TEMPLATE.format(
        name=name,
        engine=execution_engine,
        params=params_str,
        description=description or f"Pipeline {name}",
        output_file=output_json,
    )
    
    # Crear directorio si no existe
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Escribir archivo
    output_file.write_text(content, encoding="utf-8")
    
    print(f"✓ Workflow creado: {output_path}")
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
    
    print(f"✓ Pipeline compilado: {output_path}")
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
    verify_ssl: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Despliega un pipeline a Stratio Rocket vía API.
    
    Lee un archivo JSON de pipeline y lo sube a Rocket usando la API REST.
    Permite crear nuevos pipelines o actualizar existentes.
    
    Args:
        json_file: Ruta al archivo JSON del pipeline a desplegar
        rocket_url: URL base de Rocket (ej: https://rocket.example.com)
        api_token: Token de autenticación de Rocket. Si no se proporciona,
                  se buscará en la variable de entorno ROCKET_API_TOKEN
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
        >>> os.environ['ROCKET_API_TOKEN'] = 'my-token'
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
    # TODO: Implementar integración con API de Rocket
    # 
    # Pasos a implementar:
    # 1. Validar que el archivo JSON existe y es válido
    # 2. Leer el contenido del JSON
    # 3. Obtener token de API (parámetro o variable de entorno)
    # 4. Validar parámetros requeridos (project_id, group_id, etc.)
    # 5. Construir request HTTP a la API de Rocket
    # 6. Manejar autenticación (Bearer token, OAuth, etc.)
    # 7. Enviar POST/PUT según si es nuevo o actualización
    # 8. Procesar respuesta
    # 9. Retornar resultado
    #
    # API endpoints probables:
    # - POST /api/v1/projects/{project_id}/workflows
    # - PUT  /api/v1/projects/{project_id}/workflows/{workflow_id}
    # - GET  /api/v1/projects/{project_id}/workflows/{workflow_id}
    #
    # Headers requeridos:
    # - Authorization: Bearer {api_token}
    # - Content-Type: application/json
    #
    raise NotImplementedError(
        "La función push() aún no está implementada. "
        "Se requiere integración con la API de Stratio Rocket."
    )
