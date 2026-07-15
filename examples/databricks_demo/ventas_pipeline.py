"""Demo de un pipeline Rocket convertible a Databricks."""

from py2rocket import pipeline
from py2rocket.core import (
    add_columns,
    delta_output,
    filter,
    print_step,
    select,
    sql,
)


@pipeline(
    name="ventas-databricks-demo",
    execution_engine="Batch",
    params={"P_MIN_AMOUNT": "100"},
    description="Demo de migración desde Rocket hacia Databricks",
)
def workflow():
    ventas = sql(
        name="Load_Ventas",
        query="SELECT * FROM legacy.ventas",
        cache_table=True,
        priority=10,
        description="Lee las ventas desde Unity Catalog",
    )

    ventas_calculadas = add_columns(
        name="Calculate_Total",
        inputs=ventas,
        add_column_expression_list=[
            {"field": "total", "query": "quantity * unit_price"}
        ],
        priority=20,
        description="Calcula el importe total",
    )

    ventas_validas = filter(
        name="Filter_High_Value",
        inputs=ventas_calculadas,
        filter_exp="total >= {{P_MIN_AMOUNT}} AND status = 'active'",
        priority=30,
        description="Separa ventas activas de alto importe",
    )

    resultado = select(
        name="Select_Result",
        inputs=ventas_validas,
        columns="sale_id,customer_id,total,status",
        priority=40,
        description="Selecciona las columnas de salida",
    )

    delta_output(
        name="Save_Valid_Sales",
        inputs=resultado,
        save_mode="Overwrite",
        priority=50,
        description="Escribe ventas válidas en Unity Catalog",
    )

    print_step(
        name="Show_Discarded_Sales",
        inputs=ventas_validas.discarded,
        print_data=True,
        print_schema=True,
        priority=60,
        description="Muestra las ventas descartadas",
    )


if __name__ == "__main__":
    workflow()
