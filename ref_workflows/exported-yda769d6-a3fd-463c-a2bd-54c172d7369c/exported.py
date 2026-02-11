"""
Workflow generado desde JSON de Rocket

Workflow: demo
ID: dda769d6-a3fd-463c-a2bd-54c172d7369c
"""

from py2rocket import pipeline, build
from py2rocket.core.input import csv
from py2rocket.core.output import parquet_output
from py2rocket.core.transformation import filter
from py2rocket.core.transformation import trigger
from py2rocket.core.transformation import union

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
    f_datos2 = filter(
        name="F_Datos2",
        filter_exp="id >= 200",
        inputs=csv_step,
        priority=50
    )
    uniondatos = union(
        name="UnionDatos",
        inputs=[f_datos, f_datos2],
        priority=50
    )
    transformacion = trigger(
        name="Transformacion",
        sql="""
SELECT *
FROM UnionDatos
""",
        inputs=uniondatos,
        priority=50
    )

    # Output nodes
    parquet = parquet_output(
        name="Parquet",
        path="/user/data/save",
        save_mode="Overwrite",
        table_name="TABLA",
        partition_by="tipo",
        partition_overwrite=True,
        check_if_empty=True,
        inputs=transformacion,
        priority=50
    )

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "exported_rebuilt.json")
