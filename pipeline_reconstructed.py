"""
Workflow generado desde JSON de Rocket

Workflow: pl-transformacion-zp-mdp-validacionlecturatablas
ID: 7133a9b4-d4fc-4390-9aa1-802d836a2874
"""

from py2rocket import pipeline, build
from py2rocket.core.input import sql
from py2rocket.core.output import print_step


@pipeline(
    name="pl-transformacion-zp-mdp-validacionlecturatablas",
    execution_engine="Hybrid",
    workflow_id="7133a9b4-d4fc-4390-9aa1-802d836a2874"
)
def workflow():
    """
    Workflow importado desde JSON de Rocket.
    """
    # Input nodes
    load_tabla = sql(
        name="Load_Tabla",
        query="SELECT * FROM {{{P_TABLA}}}",
        priority=50
    )

    # Output nodes
    print_metadata = print_step(
        name="Print_Metadata",
        inputs=load_tabla,
        priority=50
    )


if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "pipeline_generado_rebuilt.json")
