"""
Workflow generado desde JSON de Rocket

Workflow: prueba_pasos
ID: 24353634-c5a6-4867-961f-d9d4e71b67e1
"""

from py2rocket import pipeline, build
from py2rocket.core.operations import raw_step

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
    csv = raw_step(
        name="Csv",
        class_name='CsvInputStep',
        configuration={'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}'},
        ui_configuration={'position': {'x': 90.0, 'y': 1380.0}},
        last_modified="2026-02-11T01:06:56Z",
        include_description=False
    )
    custom = raw_step(
        name="Custom",
        class_name='CustomLiteXDInputStep',
        configuration={'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'userPassEnable': False},
        arity=[],
        ui_configuration={'position': {'x': 90.0, 'y': 60.0}},
        last_modified="2026-02-11T01:05:22Z",
        include_description=False
    )
    jdbc = raw_step(
        name="Jdbc",
        class_name='JdbcInputStep',
        configuration={'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'userPassEnable': False},
        priority=50,
        ui_configuration={'position': {'x': 90.0, 'y': 170.0}},
        last_modified="2026-02-11T01:05:47Z",
        include_description=False
    )
    json = raw_step(
        name="Json",
        class_name='JsonInputStep',
        configuration={'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}'},
        ui_configuration={'position': {'x': 90.0, 'y': 1270.0}},
        last_modified="2026-02-11T01:06:54Z",
        include_description=False
    )
    parquet = raw_step(
        name="Parquet",
        class_name='ParquetInputStep',
        configuration={'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'genAIMetadataColumns': ''},
        supported_data_relations=['ValidData'],
        ui_configuration={'position': {'x': 90.0, 'y': 390.0}},
        last_modified="2026-02-11T01:06:08Z",
        include_description=False
    )
    pyspark = raw_step(
        name="PySpark",
        class_name='PySparkInputStep',
        configuration={'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'pythonCode': 'from pyspark.sql import *\nfrom pyspark.sql.functions import *\nfrom pyspark.sql.types import *\n\ndef pyspark_input(spark, param_dict):\n    """\n    :param spark: SparkSession\n    :param param_dict: Input dictionary\n    :return: Valid DataFrame\n    """\n\n    # Insert your pySpark code here\n    # ...\n\n    return output_df', 'genAIMetadataColumns': '', 'pythonInputDictionary': ''},
        execution_engine='Hybrid',
        supported_engines=['Batch', 'Hybrid'],
        supported_data_relations=['ValidData'],
        outputs_writer=[],
        ui_configuration={'position': {'x': 90.0, 'y': 610.0}},
        lineage_properties=[],
        last_modified="2026-02-11T01:06:16Z",
        include_description=False
    )
    sftp = raw_step(
        name="SFTP",
        step_type='Input',
        class_name='SFTPInputStep',
        class_pretty_name='SFTP',
        configuration={'inputOptions': '', 'path': '', 'username': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'avoidHdfsFiles': False, 'host': '', 'tlsEnabled': False, 'fileType': 'txt', 'dataSourceClass': '', 'port': '22', 'vaultSecretName': '', 'genAIMetadataColumns': '', 'vaultUserPassEnabled': False, 'schema.sparkSchema': '', 'password': ''},
        priority=50,
        arity=['NullaryToNary'],
        execution_engine='Hybrid',
        supported_engines=['Batch', 'Hybrid'],
        supported_data_relations=['ValidData'],
        outputs_writer=[],
        ui_configuration={'position': {'x': 90.0, 'y': 940.0}},
        lineage_properties=[],
        last_modified="2026-02-11T01:06:26Z",
        include_description=False
    )
    test = raw_step(
        name="Test",
        step_type='Input',
        class_name='TestInputStep',
        class_pretty_name='Test',
        configuration={'eventType': 'STRING', 'maxNumber': '', 'outputField': 'raw', 'event': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'numEvents': '10', 'explodeEvent': False, 'genAIMetadataColumns': ''},
        priority=50,
        arity=['NullaryToNary'],
        execution_engine='Hybrid',
        supported_engines=['Batch', 'Hybrid'],
        supported_data_relations=['ValidData'],
        outputs_writer=[],
        ui_configuration={'position': {'x': 90.0, 'y': 1160.0}},
        lineage_properties=[],
        last_modified="2026-02-11T01:06:37Z",
        include_description=False
    )

    # Transformation nodes
    mlmodel = raw_step(
        name="MlModel",
        step_type='Transformation',
        class_name='MlModelTransformStep',
        class_pretty_name='MlModel',
        configuration={'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'mlModelLoadingFromType': 'MlModelFromAsset', 'inputSchemas': '', 'predictionColumnName': '', 'MlModelAux': '', 'mlProjectModelSettings': False, 'postgresTimeoutSeconds': '180', 'predictionColumnType': 'StringType', 'genAIMetadataColumns': '', 'enablePostProcessing': False},
        priority=50,
        arity=['UnaryOrBinaryToNary'],
        execution_engine='Hybrid',
        supported_engines=['Batch', 'Hybrid', 'Streaming'],
        supported_data_relations=['ValidData'],
        outputs_writer=[],
        ui_configuration={'position': {'x': 90.0, 'y': 1600.0}},
        lineage_properties=[],
        last_modified="2026-02-11T01:07:05Z",
        include_description=False
    )
    pyspark_1 = raw_step(
        name="PySpark_1",
        class_name='PySparkTransformerStep',
        configuration={'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'pythonCode': 'from pyspark.sql import *\nfrom pyspark.sql.functions import *\nfrom pyspark.sql.types import *\n\n#If the step contains a single input and output \n#def pyspark_transform(spark, df, param_dict):\n    #:param spark: SparkSession\n    #:param df: Input DataFrame\n    #:param param_dict: Input dictionary\n    #:return: Transformed DataFrame OR (Valid, discarded) dataframe tuple\n\n    # Insert your pySpark code here\n    # ...\n\n#    return # output_df OR (valid_df, discarded_df)\n\n#If the step contains multiple inputs and outputs \n#def pyspark_transform(spark, dict_df, param_dict):\n    #:param spark: SparkSession\n    #:param dict_df: Input DataFrames Dictionary ["stepName", step_df]\n    #:param param_dict: Input dictionary\n    #:return: Transformed DataFrame OR (Valid, discarded) dataframe tuple\n\n    # Insert your pySpark code here\n    # ...\n\n#    return # output_df OR (valid_df, discarded_df)'},
        ui_configuration={'position': {'x': 90.0, 'y': 720.0}},
        last_modified="2026-02-11T01:06:19Z",
        include_description=False
    )
    union = raw_step(
        name="Union",
        step_type='Transformation',
        class_name='UnionTransformStep',
        class_pretty_name='Union',
        configuration={'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'inputSchemas': '', 'genAIMetadataColumns': ''},
        priority=50,
        arity=['NaryToNary'],
        execution_engine='Hybrid',
        supported_engines=['Streaming', 'Batch', 'Hybrid'],
        supported_data_relations=['ValidData'],
        outputs_writer=[],
        ui_configuration={'position': {'x': 90.0, 'y': 280.0}},
        lineage_properties=[],
        last_modified="2026-02-11T01:06:02Z",
        include_description=False
    )

    # Output nodes
    csv_1 = raw_step(
        name="Csv_1",
        class_name='CsvOutputStep',
        configuration={'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}'},
        supported_engines=['Streaming', 'Batch', 'Hybrid'],
        ui_configuration={'position': {'x': 90.0, 'y': 1490.0}},
        last_modified="2026-02-11T01:06:59Z",
        include_supported_data_relations=False,
        include_description=False
    )
    custom_1 = raw_step(
        name="Custom_1",
        class_name='CustomLiteXDOutputStep',
        configuration={'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'userPassEnable': False},
        ui_configuration={'position': {'x': 90.0, 'y': 1710.0}},
        last_modified="2026-02-11T01:07:23Z",
        include_supported_data_relations=False,
        include_description=False
    )
    jdbc_1 = raw_step(
        name="Jdbc_1",
        class_name='JdbcOutputStep',
        configuration={'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}'},
        ui_configuration={'position': {'x': 90.0, 'y': 1820.0}},
        last_modified="2026-02-11T01:07:31Z",
        include_supported_data_relations=False,
        include_description=False
    )
    parquet_1 = raw_step(
        name="Parquet_1",
        class_name='ParquetOutputStep',
        configuration={'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}'},
        ui_configuration={'position': {'x': 90.0, 'y': 500.0}},
        last_modified="2026-02-11T01:06:11Z",
        include_supported_data_relations=False,
        include_description=False
    )
    pyspark_2 = raw_step(
        name="PySpark_2",
        class_name='PySparkOutputStep',
        configuration={'pythonCode': "from pyspark.sql import *\nfrom pyspark.sql.functions import *\nfrom pyspark.sql.types import *\n\ndef pyspark_output(spark, df, write_options_dict, param_dict):\n    '''\n    :param spark: SparkSession\n    :param df: Input DataFrame\n    :param write_options_dict: Write options defined in previous step\n    :param param_dict: Key-value dictionary defined in this step\n    '''\n\n    # Insert your pySpark code here\n    # ...\n\n    return", 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}'},
        ui_configuration={'position': {'x': 90.0, 'y': 830.0}},
        last_modified="2026-02-11T01:06:24Z",
        include_supported_data_relations=False,
        include_description=False
    )
    sftp_1 = raw_step(
        name="SFTP_1",
        class_name='SFTPOutputStep',
        configuration={'path': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'avoidHdfsFiles': False, 'host': '', 'customFileType': '', 'preserveWriterFileExtension': False, 'tlsEnabled': False, 'fileType': 'txt', 'dataSourceClass': '', 'port': '22', 'vaultSecretName': '', 'sftpServerUsername': '', 'saveOptions': '', 'vaultUserPassEnabled': False, 'password': ''},
        execution_engine='Hybrid',
        supported_engines=['Streaming', 'Batch', 'Hybrid'],
        outputs_writer=[],
        ui_configuration={'position': {'x': 90.0, 'y': 1050.0}},
        lineage_properties=[],
        last_modified="2026-02-11T01:06:29Z",
        include_supported_data_relations=False,
        include_description=False
    )

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "exported_roundtrip_rebuilt_rebuilt.json")
