"""
Workflow generado desde JSON de Rocket

Workflow: prueba
ID: 1564a084-6708-4193-a883-e4e9c23d684f
"""

from py2rocket import pipeline, build
from py2rocket.core.input import sql
from py2rocket.core.output import print_step

@pipeline(
    name="prueba",
    execution_engine="Hybrid",
    workflow_id="1564a084-6708-4193-a883-e4e9c23d684f"
)
def workflow():
    """
    Workflow importado desde JSON de Rocket.
    """
    # Input nodes
    sql_step = sql(
        name="SQL",
        query="""
SELECT
    1 AS columna
""",
        priority=50
    )

    # Output nodes
    print = print_step(
        name="Print",
        inputs=sql_step,
        priority=50
    )

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "exported_rebuilt.json")
