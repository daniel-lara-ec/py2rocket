"""
DSL Decorators para Stratio Rocket

Define el decorator @pipeline que permite crear pipelines de forma declarativa.

El decorator:
1. Captura la definición del pipeline
2. Ejecuta la función para construir el DAG
3. Retorna el objeto Pipeline con el IR completo
"""

from typing import Callable, Dict, Any, Optional
from functools import wraps
from py2rocket.core.pipeline import Pipeline, ExecutionEngine
from py2rocket.core.operations import set_current_pipeline


def pipeline(
    name: str,
    execution_engine: str = "Hybrid",
    params: Optional[Dict[str, str]] = None,
    description: str = "",
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
            pipe = Pipeline(
                name=name,
                execution_engine=engine,
                parameters=params or {},
                description=description,
            )

            # Establecer como pipeline activo
            set_current_pipeline(pipe)

            # Ejecutar la función para construir el DAG
            func(*args, **kwargs)

            # Validar el pipeline
            pipe.validate()

            return pipe

        return wrapper

    return decorator
