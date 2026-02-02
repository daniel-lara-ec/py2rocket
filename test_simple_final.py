"""
Workflow generado desde JSON de Rocket

Workflow: test_simple
ID: test-simple-strings
"""

from py2rocket import pipeline, build
from py2rocket.core.input import sql
from py2rocket.core.output import print_step

@pipeline(
    name="test_simple",
    execution_engine="Batch",
    workflow_id="test-simple-strings",
    parameters_lists=['Environment']
)
def workflow():
    """
    Workflow importado desde JSON de Rocket.
    """
    # Input nodes
    simpleinput = sql(
        name="SimpleInput",
        query="SELECT * FROM tabla",
        priority=50
    )

    # Output nodes
    simpleoutput = print_step(
        name="SimpleOutput",
        inputs=simpleinput,
        priority=50
    )

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "test_simple_rebuilt.json")
