"""
Ejemplo de uso del módulo py2rocket

Este ejemplo muestra cómo usar py2rocket para crear un pipeline simple
que carga datos desde una tabla y los imprime.
"""

from py2rocket import pipeline, build
from py2rocket.core.input import sql
from py2rocket.core.output import print_step


# Definir el pipeline usando el DSL
@pipeline(
    name="pl-transformacion-zp-mdp-validacionlecturatablas",
    execution_engine="Hybrid",
    params={"P_TABLA": "prd_campanias.ZC_BP_Par_Cam_salesforce_Account"},
)
def flujo():
    """Pipeline de ejemplo para validar lectura de tablas"""

    # Paso 1: Cargar datos desde una tabla SQL
    tabla = sql(name="Load_Tabla", query="SELECT * FROM {{{P_TABLA}}}", priority=50)

    # Paso 2: Imprimir los metadatos de la tabla
    print_step(name="Print_Metadata", inputs=tabla, priority=50)


# Ejecutar la definición del pipeline
if __name__ == "__main__":
    # Crear el pipeline (ejecuta el decorator y construye el DAG)
    mi_pipeline = flujo()

    # Compilar a JSON de Rocket usando la función build
    build(mi_pipeline, "pipeline_generado.json")
