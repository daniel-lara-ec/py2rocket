"""
Workflow generado desde JSON de Rocket

Workflow: imported_workflow
"""

from py2rocket import pipeline, build

@pipeline(
    name="imported_workflow",
    execution_engine="Hybrid"
)
def workflow():
    """
    Workflow importado desde JSON de Rocket.
    """

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "info_rebuilt.json")
