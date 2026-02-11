"""
Workflow generado desde JSON de Rocket

Workflow: demo
ID: dda769d6-a3fd-463c-a2bd-54c172d7369c
"""

from py2rocket import pipeline, build
from py2rocket.core.input import csv
from py2rocket.core.output import print_step
from py2rocket.core.transformation import filter
from py2rocket.core.transformation import trigger

@pipeline(
    name="demo",
    execution_engine="Hybrid",
    workflow_id="dda769d6-a3fd-463c-a2bd-54c172d7369c"
)
def workflow():
    """
    Workflow importado desde JSON de Rocket.
    """
    # Input nodes
    csv_step = csv(
        name="Csv",
        path="/user/rocket.stratio-rocket/practica_episodio.csv",
        header=True,
        delimiter=",",
        priority=50
    )

    # Transformation nodes
    f_datos = filter(
        name="F_Datos",
        filter_exp="id < 100",
        inputs=csv_step,
        priority=50
    )
    transformacion = trigger(
        name="Transformacion",
        sql="""
SELECT *
FROM F_Datos
""",
        inputs=f_datos,
        priority=50
    )

    # Output nodes
    print = print_step(
        name="Print",
        inputs=transformacion,
        priority=50
    )

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "exported_rebuilt.json")
