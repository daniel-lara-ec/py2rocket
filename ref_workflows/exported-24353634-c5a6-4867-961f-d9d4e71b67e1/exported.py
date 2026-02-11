"""
Workflow generado desde JSON de Rocket

Workflow: prueba_pasos
ID: 24353634-c5a6-4867-961f-d9d4e71b67e1
"""

from py2rocket import pipeline, build
from py2rocket.core.input import csv
from py2rocket.core.input import custom_lite_xd
from py2rocket.core.input import jdbc
from py2rocket.core.input import json
from py2rocket.core.input import parquet
from py2rocket.core.input import pyspark_input
from py2rocket.core.output import csv_output
from py2rocket.core.output import custom_lite_xd_output
from py2rocket.core.output import jdbc_output
from py2rocket.core.output import parquet_output
from py2rocket.core.output import pyspark_output
from py2rocket.core.output import sftp_output
from py2rocket.core.transformation import pyspark

@pipeline(
    name="prueba_pasos",
    execution_engine="Hybrid",
    workflow_id="24353634-c5a6-4867-961f-d9d4e71b67e1"
)
def workflow():
    """
    Workflow importado desde JSON de Rocket.
    """
    # Input nodes
    csv_step = csv(
        name="Csv",
        is_recursive_enabled=True,
        paths=[{'path': None, 'subdirGlobFilter': None, 'subdirRegexFilter': None, 'excludeGlobFilter': None, 'excludeRegexFilter': None}],
        metadata_column_enabled=True,
        header=False,
        enable_filter_pattern=True,
        path_glob_filter="*.csv",
        delimiter=",",
        priority=50
    )
    custom = custom_lite_xd(
        name="Custom",
        user_pass_enable=False,
        is_legacy_batch_step=False,
        vault_custom_property_enabled=False,
        is_streaming=False,
        priority=50
    )
    jdbc_step = jdbc(
        name="Jdbc",
        user_pass_enable=False,
        isolation_level="READ_UNCOMMITTED",
        driver="org.postgresql.Driver",
        priority=50
    )
    json_step = json(
        name="Json",
        is_recursive_enabled=True,
        paths=[{'path': None, 'subdirGlobFilter': None, 'subdirRegexFilter': None, 'excludeGlobFilter': None, 'excludeRegexFilter': None}],
        metadata_column_enabled=True,
        enable_filter_pattern=True,
        path_glob_filter="*.json",
        priority=50
    )
    parquet_step = parquet(
        name="Parquet",
        is_recursive_enabled=True,
        paths=[{'path': None, 'subdirGlobFilter': None, 'subdirRegexFilter': None, 'excludeGlobFilter': None, 'excludeRegexFilter': None}],
        metadata_column_enabled=True,
        enable_filter_pattern=True,
        path_glob_filter="*.parquet",
        priority=50
    )
    pyspark_step = pyspark_input(
        name="PySpark",
        python_code="""
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def pyspark_input(spark, param_dict):
    \"\"\"
    :param spark: SparkSession
    :param param_dict: Input dictionary
    :return: Valid DataFrame
    \"\"\"

    # Insert your pySpark code here
    # ...

    return output_df
""",
        priority=50
    )
    # TODO: Unsupported node type: SFTPInputStep (SFTP)
    # TODO: Unsupported node type: TestInputStep (Test)

    # Transformation nodes
    # TODO: Unsupported node type: MlModelTransformStep (MlModel)
    pyspark_1 = pyspark(
        name="PySpark_1",
        code="""
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

#If the step contains a single input and output
#def pyspark_transform(spark, df, param_dict):
    #:param spark: SparkSession
    #:param df: Input DataFrame
    #:param param_dict: Input dictionary
    #:return: Transformed DataFrame OR (Valid, discarded) dataframe tuple

    # Insert your pySpark code here
    # ...

#    return # output_df OR (valid_df, discarded_df)

#If the step contains multiple inputs and outputs
#def pyspark_transform(spark, dict_df, param_dict):
    #:param spark: SparkSession
    #:param dict_df: Input DataFrames Dictionary ["stepName", step_df]
    #:param param_dict: Input dictionary
    #:return: Transformed DataFrame OR (Valid, discarded) dataframe tuple

    # Insert your pySpark code here
    # ...

#    return # output_df OR (valid_df, discarded_df)
""",
        priority=50
    )
    # TODO: Unsupported node type: UnionTransformStep (Union)

    # Output nodes
    csv_1 = csv_output(
        name="Csv_1",
        infer_schema=False,
        header=False,
        delimiter="{{{Environment.DEFAULT_DELIMITER}}}",
        priority=50
    )
    custom_1 = custom_lite_xd_output(
        name="Custom_1",
        user_pass_enable=False,
        vault_custom_property_enabled=False,
        priority=50
    )
    jdbc_1 = jdbc_output(
        name="Jdbc_1",
        create_schema_if_not_exists=False,
        batchsize="1000",
        user_pass_enable=False,
        isolation_level="READ_UNCOMMITTED",
        fail_fast=True,
        case_sensitive_enabled=True,
        schema_from_database=False,
        jdbc_save_mode="STATEMENT",
        priority=50
    )
    parquet_1 = parquet_output(
        name="Parquet_1",
        priority=50
    )
    pyspark_2 = pyspark_output(
        name="PySpark_2",
        python_code="""
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def pyspark_output(spark, df, write_options_dict, param_dict):
    '''
    :param spark: SparkSession
    :param df: Input DataFrame
    :param write_options_dict: Write options defined in previous step
    :param param_dict: Key-value dictionary defined in this step
    '''

    # Insert your pySpark code here
    # ...

    return
""",
        priority=50
    )
    sftp_1 = sftp_output(
        name="SFTP_1",
        avoid_hdfs_files=False,
        preserve_writer_file_extension=False,
        file_type="txt",
        port="22",
        vault_user_pass_enabled=False,
        priority=50
    )

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "exported_rebuilt.json")
