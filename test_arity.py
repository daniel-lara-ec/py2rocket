"""
Test para verificar que el campo arity se genera correctamente
para todos los tipos de nodos (Input, Transform, Output).
"""

from py2rocket import pipeline, build
from py2rocket.core.input import sql
from py2rocket.core.transformation import trigger, pyspark, add_columns, coalesce
from py2rocket.core.output import print_step, parquet_output


@pipeline(
    name="test-arity-pipeline",
    execution_engine="Hybrid",
    params={"P_PATH": "/data/output"},
)
def test_arity_workflow():
    """Pipeline para probar todos los tipos de arity"""

    # Input: NullaryToNary
    data = sql(name="SQL_Input", query="SELECT * FROM tabla", priority=10)

    # Transform: NaryToNary
    filtered = trigger(
        name="Trigger_Transform",
        sql="SELECT * WHERE status = 'active'",
        inputs=data,
        priority=20,
    )

    # Transform: NaryToNary
    transformed = pyspark(
        name="PySpark_Transform",
        code="df.withColumn('new_col', lit(1))",
        inputs=filtered,
        priority=30,
    )

    # Transform: UnaryToNary
    with_columns = add_columns(
        name="AddColumns_Transform",
        add_column_expression_list=[{"name": "test_col", "expression": "1"}],
        inputs=transformed,
        priority=40,
    )

    # Transform: [] (vacío)
    coalesced = coalesce(
        name="Coalesce_Transform", partitions=10, inputs=with_columns, priority=50
    )

    # Output: NullaryToNullary, NaryToNullary
    print_step(
        name="Print_Output",
        inputs=coalesced,
        print_schema=True,
        priority=60,
    )

    # Output: NullaryToNullary, NaryToNullary
    parquet_output(
        name="Parquet_Output",
        path="{{{P_PATH}}}",
        save_mode="overwrite",
        inputs=coalesced,
        priority=70,
    )


if __name__ == "__main__":
    pipeline_obj = test_arity_workflow()
    build(pipeline_obj, "test_arity.json")
    print("✅ Test arity completado. Revisar test_arity.json")
