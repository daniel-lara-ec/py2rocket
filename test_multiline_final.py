"""
Workflow generado desde JSON de Rocket

Workflow: test_multiline_sql
ID: test-multiline-sql
"""

from py2rocket import pipeline, build
from py2rocket.core.input import sql
from py2rocket.core.output import print_step
from py2rocket.core.transformation import add_columns
from py2rocket.core.transformation import pyspark

@pipeline(
    name="test_multiline_sql",
    execution_engine="Hybrid",
    workflow_id="test-multiline-sql"
)
def workflow():
    """
    Workflow importado desde JSON de Rocket.
    """
    # Input nodes
    load_data = sql(
        name="Load_Data",
        query="""
SELECT
    id,
    nombre,
    email,
    created_at
FROM usuarios
WHERE status = 'active'
    AND created_at >= '2024-01-01'
ORDER BY created_at DESC
""",
        priority=50
    )

    # Transformation nodes
    add_calculated_columns = add_columns(
        name="Add_Calculated_Columns",
        select_type="EXPRESSION",
        add_column_expression_list="""
CONCAT(nombre, ' - ', email) AS full_info,
CASE WHEN year >= 2024 THEN 'nuevo' ELSE 'antiguo' END AS tipo_usuario,
CURRENT_TIMESTAMP() AS processed_at
""",
        inputs="Transform_Data",
        priority=50
    )
    transform_data = pyspark(
        name="Transform_Data",
        python_code="""
from pyspark.sql import functions as F

# Aplicar transformaciones
df_transformed = df.withColumn(
    'email_domain',
    F.split(F.col('email'), '@')[1]
).withColumn(
    'year',
    F.year(F.col('created_at'))
)

df_output = df_transformed
""",
        inputs=load_data,
        priority=50
    )

    # Output nodes
    output_results = print_step(
        name="Output_Results",
        print_schema=True,
        log_level="info",
        inputs=add_calculated_columns,
        priority=50
    )

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "test_multiline_rebuilt.json")
