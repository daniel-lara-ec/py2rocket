"""
Descripción

Workflow generado por py2rocket
"""

from py2rocket import pipeline
from py2rocket.core.input import sql
from py2rocket.core.transformation import trigger
from py2rocket.core.output import parquet_output


@pipeline(
    name="pl-transformacion-Zp-Mdp-CargaTablas",
    execution_engine="Hybrid",
    params={"P_NOMBRE_TABLA": "ZC_DLA_Par_Cam_salesforce_Account"},
    parameters_lists=["ParamsRemesas"],
)
def workflow():
    """
    Define el flujo de procesamiento del pipeline.

    Ejemplo:
        tabla = sql(
            name="Load_Tabla",
            query="SELECT * FROM {{P_TABLA}}",
            priority=50
        )

        print_step(tabla, priority=50)
    """

    Load_TablaTrxMonetarias = sql(
        name="Load_TablaTrxMonetarias",
        query="SELECT * FROM prd_campanias.ZC_DLA_Par_Cam_salesforce_Account",
        priority=20,
    )

    t_triger_sql = """
    SELECT 
        *
        , CURRENT_TIMESTAMP() AS fecha_procesamiento
    FROM Load_TablaTrxMonetarias
    """

    T_AgregamosTimeStamp = trigger(
        name="T_AgregamosTimeStamp",
        inputs=[Load_TablaTrxMonetarias],
        priority=30,
        sql=t_triger_sql,
    )

    T_AgregamosTimeStamp.set_outputs_writer(
        table_name="ZC_DLA_Par_Cam_salesforce_Account",
        save_mode="Overwrite",
        check_if_empty=True,
        partition_overwrite_enabled=True,
        partition_by=["periodo"],
    )

    parquet_output(
        name="Po_Parquet",
        inputs=[T_AgregamosTimeStamp],
        priority=40,
        path="s3a://bucket-datalake-landing-zone-mdp/campanias/",
    )


if __name__ == "__main__":
    from py2rocket import build

    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "transformacion_Zp_Mdp_CargaTablas.json")
