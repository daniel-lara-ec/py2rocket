"""
Workflow generado desde JSON de Rocket

Workflow: test-filter-workflow
ID: 65f8b096-a169-477d-aab3-0d958e487c69
"""

from py2rocket import pipeline, build
from py2rocket.core import filter
from py2rocket.core import print_step
from py2rocket.core import sql
from py2rocket.core.pipeline import UIPosition

@pipeline(
    name="test-filter-workflow",
    execution_engine="Hybrid",
    workflow_id="65f8b096-a169-477d-aab3-0d958e487c69",
    project_id='078e27ea-a99e-4023-9b32-b2da4d116a00',
    parameters_lists=['Environment', 'SparkResources', 'SparkConfigurations']
)
def workflow():
    """
    Workflow importado desde JSON de Rocket.
    """
    # Input nodes
    load_sales = sql(
        name="Load_Sales",
        query="SELECT * FROM sales.transactions",
        force_native_query=False,
        cache_table=False,
        description='',
        priority=10,
        ui_position=UIPosition(x=612, y=289)
    )

    # Transformation nodes
    filter_active_high_value = filter(
        name="Filter_Active_High_Value",
        quote_sql=False,
        filter_exp="status = 'active' AND amount > 100",
        inputs=load_sales,
        description='',
        priority=20,
        ui_position=UIPosition(x=782, y=289)
    )

    # Output nodes
    print_filtered = print_step(
        name="Print_Filtered",
        print_data=False,
        print_schema=True,
        print_metadata=True,
        log_level="warn",
        inputs=filter_active_high_value,
        description='',
        priority=30,
        ui_position=UIPosition(x=952, y=289)
    )

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "test_filter_build_rebuilt.json")
