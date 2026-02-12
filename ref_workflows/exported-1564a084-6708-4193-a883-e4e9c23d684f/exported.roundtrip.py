"""
Workflow generado desde JSON de Rocket

Workflow: prueba
ID: 1564a084-6708-4193-a883-e4e9c23d684f
"""

from py2rocket import pipeline, build
from py2rocket.core.input import sql
from py2rocket.core.output import print_step
from py2rocket.core.pipeline import ExecutionEngine, StepType

@pipeline(
    name="prueba",
    execution_engine="Hybrid",
    workflow_id="1564a084-6708-4193-a883-e4e9c23d684f",
    project_id='7gj83a6q-t894-20ld2-45et-a4o433g4h6d1',
    group_id='4e71db55-7545-4bbf-958e-46247706885f',
    asset_id="73f54524-d40b-4528-8ced-33dcaa86a1fc",
    parameters_lists=['Environment', 'SparkResources', 'SparkConfigurations'],
    raw_settings={'global': {'executionMode': 'kubernetes', 'dockerSettings': {'driverDockerImage': '{{{SparkConfigurations.SPARK_DRIVER_DOCKER_IMAGE}}}', 'driverDockerVolumes': '{{{SparkConfigurations.SPARK_DRIVER_DOCKER_VOLUMES}}}', 'executorDockerImage': '{{{SparkConfigurations.SPARK_EXECUTOR_DOCKER_IMAGE}}}', 'executorDockerVolumes': '{{{SparkConfigurations.SPARK_EXECUTOR_DOCKER_VOLUMES}}}'}, 'userPluginsJars': [], 'parametersLists': ['Environment', 'SparkResources', 'SparkConfigurations'], 'parametersUsed': ['SparkConfigurations.DEBUG_AUTO_INFER_MAX_FILES_LIMIT', 'SparkConfigurations.DEBUG_FORCE_RUN_AS_EXECUTION_WITH_MAX_STEPS', 'SparkConfigurations.DEBUG_MAX_SIZE_COLUMN_EXAMPLES', 'SparkConfigurations.DEBUG_MOCK_DATA_LIMIT', 'SparkConfigurations.DEBUG_NUMBER_OF_COLUMN_EXAMPLES', 'SparkConfigurations.DEBUG_PROCESSING_DATA_LIMIT', 'SparkConfigurations.HEALTH_CHECK_FAILURES_TIMEOUT', 'SparkConfigurations.HEALTH_CHECK_GRACE_PERIOD_SECONDS', 'SparkConfigurations.HEALTH_CHECK_INTERVAL_SECONDS', 'SparkConfigurations.HEALTH_CHECK_MAX_CONSECUTIVE_FAILURES', 'SparkConfigurations.SPARK_DRIVER_DOCKER_IMAGE', 'SparkConfigurations.SPARK_DRIVER_DOCKER_VOLUMES', 'SparkConfigurations.SPARK_DRIVER_METRICS_SOURCES_WHITELIST', 'SparkConfigurations.SPARK_EXECUTOR_DOCKER_IMAGE', 'SparkConfigurations.SPARK_EXECUTOR_DOCKER_VOLUMES', 'SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS', 'SparkConfigurations.SPARK_EXECUTOR_METRICS_SOURCES_WHITELIST', 'SparkConfigurations.SPARK_HISTORY_SERVER_EVENT_LOG_ROTATE_MAX_FILE_SIZE', 'SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME', 'SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO', 'SparkConfigurations.SPARK_STREAMING_BLOCK_INTERVAL', 'SparkConfigurations.SPARK_STREAMING_CHECKPOINT_PATH', 'SparkConfigurations.SPARK_STREAMING_WINDOW', 'SparkResources.SPARK_DRIVER_CORES', 'SparkResources.SPARK_DRIVER_GPUS', 'SparkResources.SPARK_DRIVER_MEMORY', 'SparkResources.SPARK_EXECUTOR_CORES', 'SparkResources.SPARK_EXECUTOR_GPUS', 'SparkResources.SPARK_EXECUTOR_INSTANCES', 'SparkResources.SPARK_EXECUTOR_MEMORY', 'SparkResources.SPARK_KUBERNETES_SHUTDOWN'], 'sqlSettings': {'preExecutionSqlSentences': [], 'postExecutionSqlSentences': [], 'udfsToRegister': [], 'udafsToRegister': []}, 'kubernetesDeploymentSettings': {'gracePeriodSeconds': '{{{SparkConfigurations.HEALTH_CHECK_GRACE_PERIOD_SECONDS}}}', 'intervalSeconds': '{{{SparkConfigurations.HEALTH_CHECK_INTERVAL_SECONDS}}}', 'timeoutSeconds': '{{{SparkConfigurations.HEALTH_CHECK_FAILURES_TIMEOUT}}}', 'maxConsecutiveFailures': '{{{SparkConfigurations.HEALTH_CHECK_MAX_CONSECUTIVE_FAILURES}}}', 'imagePullPolicy': 'IfNotPresent', 'userEnvVariables': [], 'userLabels': [], 'includePostgresHealthCheck': True, 'includeHdfsHealthCheck': True, 'includeSparkHealthCheck': True, 'driverPlacements': {'addedPlacements': [], 'configurableProjectPlacementsEnabled': True}, 'executorPlacements': {'addedPlacements': [], 'configurableProjectPlacementsEnabled': True}, 'driverVolumes': {'addedVolumes': {}, 'excludedVolumes': []}, 'executorVolumes': {'addedVolumes': {}, 'excludedVolumes': []}}, 'enableQualityRules': True, 'debugSettings': {'forceDebugExecutionForAllSteps': False, 'limitRecordsDebug': '{{{SparkConfigurations.DEBUG_MOCK_DATA_LIMIT}}}', 'limitProcessingRecordsDebug': '{{{SparkConfigurations.DEBUG_PROCESSING_DATA_LIMIT}}}', 'doNotUseCacheData': True, 'unlimitedRecordsInProcessing': False, 'autoInferMaxFiles': '{{{SparkConfigurations.DEBUG_AUTO_INFER_MAX_FILES_LIMIT}}}', 'forceRunAsExecution': False, 'forceRunAsExecutionWithMaxSteps': '{{{SparkConfigurations.DEBUG_FORCE_RUN_AS_EXECUTION_WITH_MAX_STEPS}}}', 'executeWithSameExecutionMode': False, 'numberOfColumnExamples': '{{{SparkConfigurations.DEBUG_NUMBER_OF_COLUMN_EXAMPLES}}}', 'maxSizeColumnExamples': '{{{SparkConfigurations.DEBUG_MAX_SIZE_COLUMN_EXAMPLES}}}', 'executeDataAnalysisInAllSteps': True}, 'autoDebugSettings': {'enableAutoDebug': True, 'forceAutoDebugExecutionForAllSteps': False, 'doNotUseCacheData': True}, 'parametersSettings': {'userDefinedParameters': []}, 'getTotalRowsByStep': False, 'enableProjectEnvVar': True, 'executionMetricsSettings': {'customMetricLabels': []}}, 'streamingSettings': {'window': '{{{SparkConfigurations.SPARK_STREAMING_WINDOW}}}', 'backpressure': False, 'blockInterval': '{{{SparkConfigurations.SPARK_STREAMING_BLOCK_INTERVAL}}}', 'stopGracefully': True, 'checkpointSettings': {'checkpointPath': '{{{SparkConfigurations.SPARK_STREAMING_CHECKPOINT_PATH}}}', 'enableCheckpointing': True, 'autoDeleteCheckpoint': True, 'addTimeToCheckpointPath': False, 'keepSameCheckpoint': False}}, 'sparkSettings': {'sparkKerberos': True, 'sparkDataStoreTls': True, 'sparkVaultSecretList': False, 'sparkVaultSecretListNames': [], 'sparkConf': {'sparkResourcesConf': {'executorMemory': '{{{SparkResources.SPARK_EXECUTOR_MEMORY}}}', 'executorCores': '{{{SparkResources.SPARK_EXECUTOR_CORES}}}', 'driverCores': '{{{SparkResources.SPARK_DRIVER_CORES}}}', 'driverMemory': '{{{SparkResources.SPARK_DRIVER_MEMORY}}}', 'limitModeDriverCores': 'SOFT', 'limitModeDriverMemory': 'GUARANTEED', 'limitModeExecutorCores': 'SOFT', 'executorInstances': '{{{SparkResources.SPARK_EXECUTOR_INSTANCES}}}', 'enableDriverGpus': False, 'driverGpus': '{{{SparkResources.SPARK_DRIVER_GPUS}}}', 'enableExecutorGpus': False, 'executorGpus': '{{{SparkResources.SPARK_EXECUTOR_GPUS}}}'}, 'sparkHistoryServerConf': {'enableHistoryServerMonitoring': False, 'sparkHistoryServerEventLogRotateEnable': False, 'sparkHistoryServerEventLogRotateMaxFileSize': '{{{SparkConfigurations.SPARK_HISTORY_SERVER_EVENT_LOG_ROTATE_MAX_FILE_SIZE}}}'}, 'userSparkConf': [], 'sparkUser': 'root', 'logStagesProgress': False, 'hdfsTokenCache': True, 'executorExtraJavaOptions': '{{{SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS}}}', 'stopGracefullyTimeout': '{{{SparkResources.SPARK_KUBERNETES_SHUTDOWN}}}', 'sparkSchedulingConf': {'minRegisteredResourcesRatio': '{{{SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO}}}', 'maxRegisteredResourcesWaitingTime': '{{{SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME}}}'}, 'sparkMetricsConf': {'sparkMetricsEnabled': False, 'sparkDriverSourcesWhitelist': '{{{SparkConfigurations.SPARK_DRIVER_METRICS_SOURCES_WHITELIST}}}', 'sparkDriverUnregisteredMetrics': [], 'sparkExecutorSourcesWhitelist': '{{{SparkConfigurations.SPARK_EXECUTOR_METRICS_SOURCES_WHITELIST}}}', 'sparkExecutorUnregisteredMetrics': []}, 'enableProjectSparkConf': True}}, 'errorsManagement': {'genericErrorManagement': {'whenError': 'Error'}}, 'pythonEnvDefinition': {'vEnvManagementMode': 'DefaultExecutionVirtualEnv', 'condaYamlDefinition': 'name: rocket-default\n\nchannels:\n  - conda-forge\n  - nodefaults\n\ndependencies:\n  - python=3.9.*\n  - pip=25.1.*\n  - pip:\n      - mlflow==2.18.*\n      - pyarrow==14.*\n      - scikit-learn==1.*\n      - numpy==1.23.*\n      - scipy==1.*\n      - pandas==1.*\n      - petastorm==0.12.*\n      - langchain==0.3.*\n      - tiktoken==0.9.0', 'freezeAfterDebug': False, 'condaPackExtension': [], 'executeCondaUnpackAfterActivate': False, 'pySparkNativeExtensions': []}, 'structuredStreamingSettings': {}},
    raw_ui_settings={'position': {'x': -229.1655408492427, 'y': -260.08007614861947, 'k': 1.8937737740515914}},
    raw_metadata={'group': {'id': '4e71db55-7545-4bbf-958e-46247706885f', 'name': '/home/system/workspace'}, 'groupId': '4e71db55-7545-4bbf-958e-46247706885f', 'projectId': '7gj83a6q-t894-20ld2-45et-a4o433g4h6d1', 'versionSparta': '3.6.1', 'creationDate': '2026-02-11T00:54:21Z', 'lastUpdateDate': '2026-02-11T00:55:22Z', 'version': 0, 'readOnly': False, 'releaseInProgress': False, 'tags': [], 'debugMode': False, 'debugAsExecutionMaybe': False, 'normalizedName': 'prueba', 'isHybridStreaming': False, 'workflowType': 'SpartaWorkflow', 'workflowMasterId': '73f54524-d40b-4528-8ced-33dcaa86a1fc'},
    annotations=[],
    node_groups=[],
    raw_nodes_order=['SQL', 'Print'],
    raw_edges_order=[{'origin': 'SQL', 'destination': 'Print', 'dataType': 'ValidData'}],
    skip_validation=True
)
def workflow():
    """
    Workflow importado desde JSON de Rocket.
    """
    # Input nodes
    sql_step = sql(
        name="SQL",
        query="""
SELECT
    1 AS columna
""",
        force_native_query=False,
        cache_table=False,
        priority=50
    )
    sql_step.node.configuration = {'priority': '50', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'isSaved': True, 'query': 'SELECT\r\n    1 AS columna', 'forceNativeQuery': False, 'cacheTable': False, 'genAIMetadataColumns': '', 'asyncRefresh': False}
    sql_step.node.priority = 50
    sql_step.node.step_type = StepType.INPUT
    sql_step.node.class_name = "SQLInputStep"
    sql_step.node.class_pretty_name = "SQL"
    sql_step.node.supported_engines = ['Batch', 'Hybrid']
    sql_step.node.supported_data_relations = ['ValidData']
    sql_step.node.execution_engine = ExecutionEngine.HYBRID
    sql_step.node.arity = ['NullaryToNary']
    sql_step.node.ui_configuration = {'position': {'x': 470.0, 'y': 266.0}}
    sql_step.node.last_modified = "2026-02-11T00:56:25Z"
    sql_step.node.include_debug_options = True
    sql_step.node.include_supported_data_relations = True
    sql_step.node.include_description = True
    sql_step.node.outputs_writer = [{'saveMode': 'Append', 'outputStepName': 'Print', 'tableName': '', 'discardTableName': '', 'extraOptions': {'checkIfEmpty': False}}]

    # Output nodes
    print = print_step(
        name="Print",
        print_data=False,
        print_schema=False,
        print_metadata=True,
        log_level="warn",
        inputs=sql_step,
        priority=50
    )
    print.node.configuration = {'priority': '50', 'printData': False, 'printSchema': False, 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'printMetadata': True, 'logLevel': 'warn'}
    print.node.priority = 50
    print.node.step_type = StepType.OUTPUT
    print.node.class_name = "PrintOutputStep"
    print.node.class_pretty_name = "Print"
    print.node.supported_engines = ['Streaming', 'Batch', 'Hybrid']
    print.node.execution_engine = ExecutionEngine.HYBRID
    print.node.arity = ['NullaryToNullary', 'NaryToNullary']
    print.node.ui_configuration = {'position': {'x': 639.0, 'y': 267.0}}
    print.node.last_modified = "2026-02-11T00:56:07Z"
    print.node.include_debug_options = True
    print.node.include_supported_data_relations = False
    print.node.include_description = False

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "exported_rebuilt.json")
