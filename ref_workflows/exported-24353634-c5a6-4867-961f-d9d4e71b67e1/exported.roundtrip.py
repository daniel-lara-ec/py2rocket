"""
Workflow generado desde JSON de Rocket

Workflow: prueba_pasos
ID: 24353634-c5a6-4867-961f-d9d4e71b67e1
"""

from py2rocket import pipeline, build
from py2rocket.core.operations import csv
from py2rocket.core.operations import csv_output
from py2rocket.core.operations import custom_lite_xd
from py2rocket.core.operations import custom_lite_xd_output
from py2rocket.core.operations import jdbc
from py2rocket.core.operations import jdbc_output
from py2rocket.core.operations import json
from py2rocket.core.operations import ml_model
from py2rocket.core.operations import parquet
from py2rocket.core.operations import parquet_output
from py2rocket.core.operations import pyspark
from py2rocket.core.operations import pyspark_input
from py2rocket.core.operations import pyspark_output
from py2rocket.core.operations import sftp_input
from py2rocket.core.operations import sftp_output
from py2rocket.core.operations import test_input
from py2rocket.core.operations import union

@pipeline(
    name="prueba_pasos",
    execution_engine="Hybrid",
    workflow_id="24353634-c5a6-4867-961f-d9d4e71b67e1",
    project_id='7gj83a6q-t894-20ld2-45et-a4o433g4h6d1',
    group_id='4e71db55-7545-4bbf-958e-46247706885f',
    asset_id="2e568d45-5ada-430e-b62d-73dc4ee42a9a",
    parameters_lists=['Environment', 'SparkResources', 'SparkConfigurations'],
    raw_settings={'global': {'executionMode': 'kubernetes', 'dockerSettings': {'driverDockerImage': '{{{SparkConfigurations.SPARK_DRIVER_DOCKER_IMAGE}}}', 'driverDockerVolumes': '{{{SparkConfigurations.SPARK_DRIVER_DOCKER_VOLUMES}}}', 'executorDockerImage': '{{{SparkConfigurations.SPARK_EXECUTOR_DOCKER_IMAGE}}}', 'executorDockerVolumes': '{{{SparkConfigurations.SPARK_EXECUTOR_DOCKER_VOLUMES}}}'}, 'userPluginsJars': [], 'parametersLists': ['Environment', 'SparkResources', 'SparkConfigurations'], 'parametersUsed': ['Environment.DEFAULT_DELIMITER', 'SparkConfigurations.DEBUG_AUTO_INFER_MAX_FILES_LIMIT', 'SparkConfigurations.DEBUG_FORCE_RUN_AS_EXECUTION_WITH_MAX_STEPS', 'SparkConfigurations.DEBUG_MAX_SIZE_COLUMN_EXAMPLES', 'SparkConfigurations.DEBUG_MOCK_DATA_LIMIT', 'SparkConfigurations.DEBUG_NUMBER_OF_COLUMN_EXAMPLES', 'SparkConfigurations.DEBUG_PROCESSING_DATA_LIMIT', 'SparkConfigurations.HEALTH_CHECK_FAILURES_TIMEOUT', 'SparkConfigurations.HEALTH_CHECK_GRACE_PERIOD_SECONDS', 'SparkConfigurations.HEALTH_CHECK_INTERVAL_SECONDS', 'SparkConfigurations.HEALTH_CHECK_MAX_CONSECUTIVE_FAILURES', 'SparkConfigurations.SPARK_DRIVER_DOCKER_IMAGE', 'SparkConfigurations.SPARK_DRIVER_DOCKER_VOLUMES', 'SparkConfigurations.SPARK_DRIVER_METRICS_SOURCES_WHITELIST', 'SparkConfigurations.SPARK_EXECUTOR_DOCKER_IMAGE', 'SparkConfigurations.SPARK_EXECUTOR_DOCKER_VOLUMES', 'SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS', 'SparkConfigurations.SPARK_EXECUTOR_METRICS_SOURCES_WHITELIST', 'SparkConfigurations.SPARK_HISTORY_SERVER_EVENT_LOG_ROTATE_MAX_FILE_SIZE', 'SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME', 'SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO', 'SparkConfigurations.SPARK_STREAMING_BLOCK_INTERVAL', 'SparkConfigurations.SPARK_STREAMING_CHECKPOINT_PATH', 'SparkConfigurations.SPARK_STREAMING_WINDOW', 'SparkResources.SPARK_DRIVER_CORES', 'SparkResources.SPARK_DRIVER_GPUS', 'SparkResources.SPARK_DRIVER_MEMORY', 'SparkResources.SPARK_EXECUTOR_CORES', 'SparkResources.SPARK_EXECUTOR_GPUS', 'SparkResources.SPARK_EXECUTOR_INSTANCES', 'SparkResources.SPARK_EXECUTOR_MEMORY', 'SparkResources.SPARK_KUBERNETES_SHUTDOWN'], 'sqlSettings': {'preExecutionSqlSentences': [], 'postExecutionSqlSentences': [], 'udfsToRegister': [], 'udafsToRegister': []}, 'kubernetesDeploymentSettings': {'gracePeriodSeconds': '{{{SparkConfigurations.HEALTH_CHECK_GRACE_PERIOD_SECONDS}}}', 'intervalSeconds': '{{{SparkConfigurations.HEALTH_CHECK_INTERVAL_SECONDS}}}', 'timeoutSeconds': '{{{SparkConfigurations.HEALTH_CHECK_FAILURES_TIMEOUT}}}', 'maxConsecutiveFailures': '{{{SparkConfigurations.HEALTH_CHECK_MAX_CONSECUTIVE_FAILURES}}}', 'imagePullPolicy': 'IfNotPresent', 'userEnvVariables': [], 'userLabels': [], 'includePostgresHealthCheck': True, 'includeHdfsHealthCheck': True, 'includeSparkHealthCheck': True, 'driverPlacements': {'addedPlacements': [], 'configurableProjectPlacementsEnabled': True}, 'executorPlacements': {'addedPlacements': [], 'configurableProjectPlacementsEnabled': True}, 'driverVolumes': {'addedVolumes': {}, 'excludedVolumes': []}, 'executorVolumes': {'addedVolumes': {}, 'excludedVolumes': []}}, 'enableQualityRules': True, 'debugSettings': {'forceDebugExecutionForAllSteps': False, 'limitRecordsDebug': '{{{SparkConfigurations.DEBUG_MOCK_DATA_LIMIT}}}', 'limitProcessingRecordsDebug': '{{{SparkConfigurations.DEBUG_PROCESSING_DATA_LIMIT}}}', 'doNotUseCacheData': True, 'unlimitedRecordsInProcessing': False, 'autoInferMaxFiles': '{{{SparkConfigurations.DEBUG_AUTO_INFER_MAX_FILES_LIMIT}}}', 'forceRunAsExecution': False, 'forceRunAsExecutionWithMaxSteps': '{{{SparkConfigurations.DEBUG_FORCE_RUN_AS_EXECUTION_WITH_MAX_STEPS}}}', 'executeWithSameExecutionMode': False, 'numberOfColumnExamples': '{{{SparkConfigurations.DEBUG_NUMBER_OF_COLUMN_EXAMPLES}}}', 'maxSizeColumnExamples': '{{{SparkConfigurations.DEBUG_MAX_SIZE_COLUMN_EXAMPLES}}}', 'executeDataAnalysisInAllSteps': True}, 'autoDebugSettings': {'enableAutoDebug': True, 'forceAutoDebugExecutionForAllSteps': False, 'doNotUseCacheData': True}, 'parametersSettings': {'userDefinedParameters': []}, 'getTotalRowsByStep': False, 'enableProjectEnvVar': True, 'executionMetricsSettings': {'customMetricLabels': []}}, 'streamingSettings': {'window': '{{{SparkConfigurations.SPARK_STREAMING_WINDOW}}}', 'backpressure': False, 'blockInterval': '{{{SparkConfigurations.SPARK_STREAMING_BLOCK_INTERVAL}}}', 'stopGracefully': True, 'checkpointSettings': {'checkpointPath': '{{{SparkConfigurations.SPARK_STREAMING_CHECKPOINT_PATH}}}', 'enableCheckpointing': True, 'autoDeleteCheckpoint': True, 'addTimeToCheckpointPath': False, 'keepSameCheckpoint': False}}, 'sparkSettings': {'sparkKerberos': True, 'sparkDataStoreTls': True, 'sparkVaultSecretList': False, 'sparkVaultSecretListNames': [], 'sparkConf': {'sparkResourcesConf': {'executorMemory': '{{{SparkResources.SPARK_EXECUTOR_MEMORY}}}', 'executorCores': '{{{SparkResources.SPARK_EXECUTOR_CORES}}}', 'driverCores': '{{{SparkResources.SPARK_DRIVER_CORES}}}', 'driverMemory': '{{{SparkResources.SPARK_DRIVER_MEMORY}}}', 'limitModeDriverCores': 'SOFT', 'limitModeDriverMemory': 'GUARANTEED', 'limitModeExecutorCores': 'SOFT', 'executorInstances': '{{{SparkResources.SPARK_EXECUTOR_INSTANCES}}}', 'enableDriverGpus': False, 'driverGpus': '{{{SparkResources.SPARK_DRIVER_GPUS}}}', 'enableExecutorGpus': False, 'executorGpus': '{{{SparkResources.SPARK_EXECUTOR_GPUS}}}'}, 'sparkHistoryServerConf': {'enableHistoryServerMonitoring': False, 'sparkHistoryServerEventLogRotateEnable': False, 'sparkHistoryServerEventLogRotateMaxFileSize': '{{{SparkConfigurations.SPARK_HISTORY_SERVER_EVENT_LOG_ROTATE_MAX_FILE_SIZE}}}'}, 'userSparkConf': [], 'sparkUser': 'root', 'logStagesProgress': False, 'hdfsTokenCache': True, 'executorExtraJavaOptions': '{{{SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS}}}', 'stopGracefullyTimeout': '{{{SparkResources.SPARK_KUBERNETES_SHUTDOWN}}}', 'sparkSchedulingConf': {'minRegisteredResourcesRatio': '{{{SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO}}}', 'maxRegisteredResourcesWaitingTime': '{{{SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME}}}'}, 'sparkMetricsConf': {'sparkMetricsEnabled': False, 'sparkDriverSourcesWhitelist': '{{{SparkConfigurations.SPARK_DRIVER_METRICS_SOURCES_WHITELIST}}}', 'sparkDriverUnregisteredMetrics': [], 'sparkExecutorSourcesWhitelist': '{{{SparkConfigurations.SPARK_EXECUTOR_METRICS_SOURCES_WHITELIST}}}', 'sparkExecutorUnregisteredMetrics': []}, 'enableProjectSparkConf': True}}, 'errorsManagement': {'genericErrorManagement': {'whenError': 'Error'}}, 'pythonEnvDefinition': {'vEnvManagementMode': 'DefaultExecutionVirtualEnv', 'condaYamlDefinition': 'name: rocket-default\n\nchannels:\n  - conda-forge\n  - nodefaults\n\ndependencies:\n  - python=3.9.*\n  - pip=25.1.*\n  - pip:\n      - mlflow==2.18.*\n      - pyarrow==14.*\n      - scikit-learn==1.*\n      - numpy==1.23.*\n      - scipy==1.*\n      - pandas==1.*\n      - petastorm==0.12.*\n      - langchain==0.3.*\n      - tiktoken==0.9.0', 'freezeAfterDebug': False, 'condaPackExtension': [], 'executeCondaUnpackAfterActivate': False, 'pySparkNativeExtensions': []}, 'structuredStreamingSettings': {}},
    raw_ui_settings={'position': {'x': 863.4905660377359, 'y': 30.754716981132077, 'k': 0.38443396226415094}},
    raw_metadata={'group': {'id': '4e71db55-7545-4bbf-958e-46247706885f', 'name': '/home/system/workspace'}, 'groupId': '4e71db55-7545-4bbf-958e-46247706885f', 'projectId': '7gj83a6q-t894-20ld2-45et-a4o433g4h6d1', 'versionSparta': '3.6.1', 'creationDate': '2026-02-11T01:03:59Z', 'lastUpdateDate': '2026-02-11T01:06:29Z', 'version': 0, 'readOnly': False, 'releaseInProgress': False, 'tags': [], 'debugMode': False, 'debugAsExecutionMaybe': False, 'normalizedName': 'prueba-pasos', 'isHybridStreaming': False, 'workflowType': 'SpartaWorkflow', 'workflowMasterId': '2e568d45-5ada-430e-b62d-73dc4ee42a9a'},
    annotations=[],
    node_groups=[],
    raw_nodes_order=['Custom', 'Jdbc', 'Union', 'Parquet', 'Parquet_1', 'PySpark', 'PySpark_1', 'PySpark_2', 'SFTP', 'SFTP_1', 'Test', 'Json', 'Csv', 'Csv_1', 'MlModel', 'Custom_1', 'Jdbc_1'],
    skip_validation=True
)
def workflow():
    """
    Workflow importado desde JSON de Rocket.
    """
    # Input nodes
    csv_step = csv(
        name="Csv",
        data_as_json_enabled=True,
        path="",
        is_recursive_enabled=True,
        paths=[{'path': None, 'subdirGlobFilter': None, 'subdirRegexFilter': None, 'excludeGlobFilter': None, 'excludeRegexFilter': None}],
        metadata_column_enabled=True,
        header=False,
        enable_filter_pattern=True,
        path_glob_filter="*.csv",
        delimiter=",",
        description='',
        config_override={'schema.inputMode': 'NOSCHEMAPROVIDED', 'excludeGlobFilter': '', 'inputOptions': '', 'dataAsJsonEnabled': True, 'path': '', 'subdirGlobFilter': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'schema.header': '', 'subdirRegexFilter': '', 'isRecursiveEnabled': True, 'readMode': 'DefaultReadMode', 'paths': [{'path': None, 'subdirGlobFilter': None, 'subdirRegexFilter': None, 'excludeGlobFilter': None, 'excludeRegexFilter': None}], 'metadataColumnEnabled': True, 'excludeRegexFilter': '', 'header': False, 'schema.fields': '', 'enableFilterPattern': True, 'genAIMetadataColumns': '', 'schema.sparkSchema': '', 'pathGlobFilter': '*.csv', 'delimiter': ','},
        node_overrides={'class_name': 'CsvInputStep', 'class_pretty_name': 'Csv', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 1380.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:06:56Z', 'include_description': False}
    )
    custom = custom_lite_xd(
        name="Custom",
        vault_db_name="",
        input_options="",
        custom_lite_class_type="",
        user_pass_enabled=False,
        is_legacy_batch_step=False,
        tls_enabled=False,
        vault_custom_property_enabled=False,
        is_streaming=False,
        description='',
        config_override={'vaultDbName': '', 'inputOptions': '', 'customLiteClassType': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'lineage_custom': '', 'userPassEnable': False, 'isLegacyBatchStep': False, 'tlsEnabled': False, 'vaultCustomPropertyEnabled': False, 'isStreaming': False, 'genAIMetadataColumns': '', 'vaultCustomPropertyName': ''},
        node_overrides={'class_name': 'CustomLiteXDInputStep', 'class_pretty_name': 'CustomLiteXD', 'supported_engines': ['Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 60.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:05:22Z', 'include_description': False}
    )
    jdbc_step = jdbc(
        name="Jdbc",
        vault_db_name="",
        input_options="",
        url="",
        user_pass_enabled=False,
        isolation_level="READ_UNCOMMITTED",
        tls_enabled=False,
        driver="org.postgresql.Driver",
        dbtable="",
        description='',
        priority=50,
        config_override={'vaultDbName': '', 'inputOptions': '', 'url': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'userPassEnable': False, 'isolationLevel': 'READ_UNCOMMITTED', 'tlsEnabled': False, 'driver': 'org.postgresql.Driver', 'dbtable': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'JdbcInputStep', 'class_pretty_name': 'Jdbc', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 170.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:05:47Z', 'include_description': False}
    )
    json_step = json(
        name="Json",
        path="",
        is_recursive_enabled=True,
        paths=[{'path': None, 'subdirGlobFilter': None, 'subdirRegexFilter': None, 'excludeGlobFilter': None, 'excludeRegexFilter': None}],
        metadata_column_enabled=True,
        enable_filter_pattern=True,
        path_glob_filter="*.json",
        description='',
        config_override={'schema.inputMode': 'NOSCHEMAPROVIDED', 'excludeGlobFilter': '', 'inputOptions': '', 'dataAsJsonEnabled': True, 'path': '', 'subdirGlobFilter': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'subdirRegexFilter': '', 'isRecursiveEnabled': True, 'readMode': 'DefaultReadMode', 'paths': [{'path': None, 'subdirGlobFilter': None, 'subdirRegexFilter': None, 'excludeGlobFilter': None, 'excludeRegexFilter': None}], 'metadataColumnEnabled': True, 'excludeRegexFilter': '', 'schema.provided': '', 'enableFilterPattern': True, 'genAIMetadataColumns': '', 'pathGlobFilter': '*.json'},
        node_overrides={'class_name': 'JsonInputStep', 'class_pretty_name': 'Json', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 1270.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:06:54Z', 'include_description': False}
    )
    parquet_step = parquet(
        name="Parquet",
        path="",
        is_recursive_enabled=True,
        paths=[{'path': None, 'subdirGlobFilter': None, 'subdirRegexFilter': None, 'excludeGlobFilter': None, 'excludeRegexFilter': None}],
        metadata_column_enabled=True,
        enable_filter_pattern=True,
        path_glob_filter="*.parquet",
        description='',
        config_override={'excludeGlobFilter': '', 'inputOptions': '', 'dataAsJsonEnabled': True, 'path': '', 'subdirGlobFilter': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'subdirRegexFilter': '', 'isRecursiveEnabled': True, 'readMode': 'DefaultReadMode', 'paths': [{'path': None, 'subdirGlobFilter': None, 'subdirRegexFilter': None, 'excludeGlobFilter': None, 'excludeRegexFilter': None}], 'metadataColumnEnabled': True, 'excludeRegexFilter': '', 'enableFilterPattern': True, 'genAIMetadataColumns': '', 'schema.sparkSchema': '', 'pathGlobFilter': '*.parquet'},
        node_overrides={'class_name': 'ParquetInputStep', 'class_pretty_name': 'Parquet', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 390.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:06:08Z', 'include_description': False}
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
        python_input_dictionary="",
        description='',
        priority=50,
        config_override={'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'pythonCode': 'from pyspark.sql import *\nfrom pyspark.sql.functions import *\nfrom pyspark.sql.types import *\n\ndef pyspark_input(spark, param_dict):\n    """\n    :param spark: SparkSession\n    :param param_dict: Input dictionary\n    :return: Valid DataFrame\n    """\n\n    # Insert your pySpark code here\n    # ...\n\n    return output_df', 'genAIMetadataColumns': '', 'pythonInputDictionary': ''},
        node_overrides={'class_name': 'PySparkInputStep', 'class_pretty_name': 'PySpark', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 610.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:06:16Z', 'include_description': False}
    )
    sftp = sftp_input(
        name="SFTP",
        input_options="",
        path="",
        username="",
        avoid_hdfs_files=False,
        host="",
        tls_enabled=False,
        file_type="txt",
        data_source_class="",
        port="22",
        vault_secret_name="",
        vault_user_pass_enabled=False,
        password="",
        description='',
        priority=50,
        config_override={'inputOptions': '', 'path': '', 'username': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'avoidHdfsFiles': False, 'host': '', 'tlsEnabled': False, 'fileType': 'txt', 'dataSourceClass': '', 'port': '22', 'vaultSecretName': '', 'genAIMetadataColumns': '', 'vaultUserPassEnabled': False, 'schema.sparkSchema': '', 'password': ''},
        node_overrides={'class_name': 'SFTPInputStep', 'class_pretty_name': 'SFTP', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 940.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:06:26Z', 'include_description': False}
    )
    test = test_input(
        name="Test",
        event_type="STRING",
        max_number="",
        output_field="raw",
        event="",
        num_events="10",
        explode_event=False,
        description='',
        priority=50,
        config_override={'eventType': 'STRING', 'maxNumber': '', 'outputField': 'raw', 'event': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'numEvents': '10', 'explodeEvent': False, 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'TestInputStep', 'class_pretty_name': 'Test', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 1160.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:06:37Z', 'include_description': False}
    )

    # Transformation nodes
    mlmodel = ml_model(
        name="MlModel",
        ml_model_loading_from_type="MlModelFromAsset",
        prediction_column_name="",
        ml_model_aux="",
        ml_project_model_settings=False,
        postgres_timeout_seconds="180",
        prediction_column_type="StringType",
        enable_post_processing=False,
        description='',
        priority=50,
        config_override={'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'mlModelLoadingFromType': 'MlModelFromAsset', 'inputSchemas': '', 'predictionColumnName': '', 'MlModelAux': '', 'mlProjectModelSettings': False, 'postgresTimeoutSeconds': '180', 'predictionColumnType': 'StringType', 'genAIMetadataColumns': '', 'enablePostProcessing': False},
        node_overrides={'class_name': 'MlModelTransformStep', 'class_pretty_name': 'MlModel', 'supported_engines': ['Batch', 'Hybrid', 'Streaming'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 1600.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:07:05Z', 'include_description': False}
    )
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
        description='',
        config_override={'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'pythonCode': 'from pyspark.sql import *\nfrom pyspark.sql.functions import *\nfrom pyspark.sql.types import *\n\n#If the step contains a single input and output \n#def pyspark_transform(spark, df, param_dict):\n    #:param spark: SparkSession\n    #:param df: Input DataFrame\n    #:param param_dict: Input dictionary\n    #:return: Transformed DataFrame OR (Valid, discarded) dataframe tuple\n\n    # Insert your pySpark code here\n    # ...\n\n#    return # output_df OR (valid_df, discarded_df)\n\n#If the step contains multiple inputs and outputs \n#def pyspark_transform(spark, dict_df, param_dict):\n    #:param spark: SparkSession\n    #:param dict_df: Input DataFrames Dictionary ["stepName", step_df]\n    #:param param_dict: Input dictionary\n    #:return: Transformed DataFrame OR (Valid, discarded) dataframe tuple\n\n    # Insert your pySpark code here\n    # ...\n\n#    return # output_df OR (valid_df, discarded_df)', 'genAIMetadataColumns': '', 'pythonInputDictionary': ''},
        node_overrides={'class_name': 'PySparkTransformerStep', 'class_pretty_name': 'PySpark', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'supported_data_relations': ['ValidData', 'DiscardedData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 720.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:06:19Z', 'include_description': False}
    )
    union_step = union(
        name="Union",
        inputs=[],
        description='',
        priority=50,
        config_override={'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'inputSchemas': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'UnionTransformStep', 'class_pretty_name': 'Union', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 280.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:06:02Z', 'include_description': False}
    )

    # Output nodes
    csv_1 = csv_output(
        name="Csv_1",
        path="",
        infer_schema=False,
        header=False,
        save_options="",
        delimiter="{{{Environment.DEFAULT_DELIMITER}}}",
        inputs=[],
        description='',
        config_override={'path': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'inferSchema': False, 'header': False, 'saveOptions': '', 'delimiter': '{{{Environment.DEFAULT_DELIMITER}}}'},
        node_overrides={'class_name': 'CsvOutputStep', 'class_pretty_name': 'Csv', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 1490.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:06:59Z', 'include_supported_data_relations': False, 'include_description': False}
    )
    custom_1 = custom_lite_xd_output(
        name="Custom_1",
        vault_db_name="",
        custom_lite_class_type="",
        user_pass_enabled=False,
        tls_enabled=False,
        vault_custom_property_enabled=False,
        inputs=[],
        description='',
        config_override={'vaultDbName': '', 'customLiteClassType': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'lineage_custom': '', 'userPassEnable': False, 'tlsEnabled': False, 'vaultCustomPropertyEnabled': False, 'saveOptions': '', 'vaultCustomPropertyName': ''},
        node_overrides={'class_name': 'CustomLiteXDOutputStep', 'class_pretty_name': 'CustomLiteXD', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 1710.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:07:23Z', 'include_supported_data_relations': False, 'include_description': False}
    )
    jdbc_1 = jdbc_output(
        name="Jdbc_1",
        url="",
        create_schema_if_not_exists=False,
        user_pass_enabled=False,
        isolation_level="READ_UNCOMMITTED",
        fail_fast=True,
        tls_enabled=False,
        driver="",
        case_sensitive_enabled=True,
        schema_from_database=False,
        jdbc_save_mode="STATEMENT",
        save_options="",
        inputs=[],
        description='',
        config_override={'vaultDbName': '', 'url': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'createSchemaIfNotExists': False, 'batchsize': '1000', 'userPassEnable': False, 'isolationLevel': 'READ_UNCOMMITTED', 'failFast': True, 'tlsEnabled': False, 'driver': '', 'caseSensitiveEnabled': True, 'schemaFromDatabase': False, 'jdbcSaveMode': 'STATEMENT', 'saveOptions': ''},
        node_overrides={'class_name': 'JdbcOutputStep', 'class_pretty_name': 'Jdbc', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 1820.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:07:31Z', 'include_supported_data_relations': False, 'include_description': False}
    )
    parquet_1 = parquet_output(
        name="Parquet_1",
        path="",
        save_options="",
        inputs=[],
        description='',
        config_override={'path': '', 'saveOptions': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}'},
        node_overrides={'class_name': 'ParquetOutputStep', 'class_pretty_name': 'Parquet', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 500.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:06:11Z', 'include_supported_data_relations': False, 'include_description': False}
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
        inputs=[],
        description='',
        config_override={'pythonCode': "from pyspark.sql import *\nfrom pyspark.sql.functions import *\nfrom pyspark.sql.types import *\n\ndef pyspark_output(spark, df, write_options_dict, param_dict):\n    '''\n    :param spark: SparkSession\n    :param df: Input DataFrame\n    :param write_options_dict: Write options defined in previous step\n    :param param_dict: Key-value dictionary defined in this step\n    '''\n\n    # Insert your pySpark code here\n    # ...\n\n    return", 'pythonInputDictionary': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}'},
        node_overrides={'class_name': 'PySparkOutputStep', 'class_pretty_name': 'PySpark', 'supported_engines': ['Batch', 'Hybrid', 'Streaming'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 830.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:06:24Z', 'include_supported_data_relations': False, 'include_description': False}
    )
    sftp_1 = sftp_output(
        name="SFTP_1",
        path="",
        avoid_hdfs_files=False,
        host="",
        custom_file_type="",
        preserve_writer_file_extension=False,
        tls_enabled=False,
        file_type="txt",
        data_source_class="",
        port="22",
        vault_secret_name="",
        sftp_server_username="",
        save_options="",
        vault_user_pass_enabled=False,
        password="",
        inputs=[],
        description='',
        priority=50,
        config_override={'path': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'avoidHdfsFiles': False, 'host': '', 'customFileType': '', 'preserveWriterFileExtension': False, 'tlsEnabled': False, 'fileType': 'txt', 'dataSourceClass': '', 'port': '22', 'vaultSecretName': '', 'sftpServerUsername': '', 'saveOptions': '', 'vaultUserPassEnabled': False, 'password': ''},
        node_overrides={'class_name': 'SFTPOutputStep', 'class_pretty_name': 'SFTP', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 90.0, 'y': 1050.0}}, 'lineage_properties': [], 'last_modified': '2026-02-11T01:06:29Z', 'include_supported_data_relations': False, 'include_description': False}
    )

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "exported_rebuilt.json")
