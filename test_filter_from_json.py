"""
Workflow generado desde JSON de Rocket

Workflow: test-filter-workflow
ID: c3b506b3-785d-474a-a6cd-2ce3e3d74f88
"""

from py2rocket import pipeline, build
from py2rocket.core import filter
from py2rocket.core import print_step
from py2rocket.core import sql
from py2rocket.core.pipeline import PythonEnvDefinition
from py2rocket.core.pipeline import UIPosition

@pipeline(
    name="test-filter-workflow",
    execution_engine="Hybrid",
    version=0,
    workflow_id="c3b506b3-785d-474a-a6cd-2ce3e3d74f88",
    project_id='078e27ea-a99e-4023-9b32-b2da4d116a00',
    parameters_lists=['Environment', 'SparkResources', 'SparkConfigurations'],
    python_env_definition=PythonEnvDefinition(v_env_management_mode='DefaultExecutionVirtualEnv', conda_yaml_definition="name: rocket-default\n\nchannels:\n  - conda-forge\n  - nodefaults\n\ndependencies:\n  - python=3.9.*\n  - pip=25.1.*\n  - pip:\n      - mlflow==2.18.*\n      - pyarrow==14.*\n      - scikit-learn==1.*\n      - numpy==1.23.*\n      - scipy==1.*\n      - pandas==1.*\n      - petastorm==0.12.*\n      - langchain==0.3.*\n      - tiktoken==0.9.0", freeze_after_debug=False, conda_pack_extension=[], execute_conda_unpack_after_activate=False, py_spark_native_extensions=[]),
    ui_settings={'position': {'x': 782.0, 'y': 289.0, 'k': 1.4285714285714286}}
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
