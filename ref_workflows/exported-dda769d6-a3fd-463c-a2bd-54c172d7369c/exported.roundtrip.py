"""
Workflow generado desde JSON de Rocket

Workflow: demo
ID: dda769d6-a3fd-463c-a2bd-54c172d7369c
"""

from py2rocket import pipeline, build
from py2rocket.core.input import csv
from py2rocket.core.output import print_step
from py2rocket.core.pipeline import ExecutionEngine, StepType
from py2rocket.core.transformation import filter
from py2rocket.core.transformation import trigger

@pipeline(
    name="demo",
    execution_engine="Hybrid",
    workflow_id="dda769d6-a3fd-463c-a2bd-54c172d7369c",
    project_id='7gj83a6q-t894-20ld2-45et-a4o433g4h6d1',
    group_id='4e71db55-7545-4bbf-958e-46247706885f',
    asset_id="5c1d1353-a2b7-4067-88e1-0bce2c5adbe8",
    parameters_lists=['Environment', 'SparkResources', 'SparkConfigurations'],
    raw_settings={'global': {'executionMode': 'kubernetes', 'dockerSettings': {'driverDockerImage': '{{{SparkConfigurations.SPARK_DRIVER_DOCKER_IMAGE}}}', 'driverDockerVolumes': '{{{SparkConfigurations.SPARK_DRIVER_DOCKER_VOLUMES}}}', 'executorDockerImage': '{{{SparkConfigurations.SPARK_EXECUTOR_DOCKER_IMAGE}}}', 'executorDockerVolumes': '{{{SparkConfigurations.SPARK_EXECUTOR_DOCKER_VOLUMES}}}'}, 'userPluginsJars': [], 'parametersLists': ['Environment', 'SparkResources', 'SparkConfigurations'], 'parametersUsed': ['SparkConfigurations.DEBUG_AUTO_INFER_MAX_FILES_LIMIT', 'SparkConfigurations.DEBUG_FORCE_RUN_AS_EXECUTION_WITH_MAX_STEPS', 'SparkConfigurations.DEBUG_MAX_SIZE_COLUMN_EXAMPLES', 'SparkConfigurations.DEBUG_MOCK_DATA_LIMIT', 'SparkConfigurations.DEBUG_NUMBER_OF_COLUMN_EXAMPLES', 'SparkConfigurations.DEBUG_PROCESSING_DATA_LIMIT', 'SparkConfigurations.HEALTH_CHECK_FAILURES_TIMEOUT', 'SparkConfigurations.HEALTH_CHECK_GRACE_PERIOD_SECONDS', 'SparkConfigurations.HEALTH_CHECK_INTERVAL_SECONDS', 'SparkConfigurations.HEALTH_CHECK_MAX_CONSECUTIVE_FAILURES', 'SparkConfigurations.SPARK_DRIVER_DOCKER_IMAGE', 'SparkConfigurations.SPARK_DRIVER_DOCKER_VOLUMES', 'SparkConfigurations.SPARK_DRIVER_METRICS_SOURCES_WHITELIST', 'SparkConfigurations.SPARK_EXECUTOR_DOCKER_IMAGE', 'SparkConfigurations.SPARK_EXECUTOR_DOCKER_VOLUMES', 'SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS', 'SparkConfigurations.SPARK_EXECUTOR_METRICS_SOURCES_WHITELIST', 'SparkConfigurations.SPARK_HISTORY_SERVER_EVENT_LOG_ROTATE_MAX_FILE_SIZE', 'SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME', 'SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO', 'SparkConfigurations.SPARK_STREAMING_BLOCK_INTERVAL', 'SparkConfigurations.SPARK_STREAMING_CHECKPOINT_PATH', 'SparkConfigurations.SPARK_STREAMING_WINDOW', 'SparkResources.SPARK_DRIVER_CORES', 'SparkResources.SPARK_DRIVER_GPUS', 'SparkResources.SPARK_DRIVER_MEMORY', 'SparkResources.SPARK_EXECUTOR_CORES', 'SparkResources.SPARK_EXECUTOR_GPUS', 'SparkResources.SPARK_EXECUTOR_INSTANCES', 'SparkResources.SPARK_EXECUTOR_MEMORY', 'SparkResources.SPARK_KUBERNETES_SHUTDOWN'], 'sqlSettings': {'preExecutionSqlSentences': [], 'postExecutionSqlSentences': [], 'udfsToRegister': [], 'udafsToRegister': []}, 'kubernetesDeploymentSettings': {'gracePeriodSeconds': '{{{SparkConfigurations.HEALTH_CHECK_GRACE_PERIOD_SECONDS}}}', 'intervalSeconds': '{{{SparkConfigurations.HEALTH_CHECK_INTERVAL_SECONDS}}}', 'timeoutSeconds': '{{{SparkConfigurations.HEALTH_CHECK_FAILURES_TIMEOUT}}}', 'maxConsecutiveFailures': '{{{SparkConfigurations.HEALTH_CHECK_MAX_CONSECUTIVE_FAILURES}}}', 'imagePullPolicy': 'IfNotPresent', 'userEnvVariables': [], 'userLabels': [], 'includePostgresHealthCheck': True, 'includeHdfsHealthCheck': True, 'includeSparkHealthCheck': True, 'driverPlacements': {'addedPlacements': [], 'configurableProjectPlacementsEnabled': True}, 'executorPlacements': {'addedPlacements': [], 'configurableProjectPlacementsEnabled': True}, 'driverVolumes': {'addedVolumes': {}, 'excludedVolumes': []}, 'executorVolumes': {'addedVolumes': {}, 'excludedVolumes': []}}, 'enableQualityRules': True, 'debugSettings': {'forceDebugExecutionForAllSteps': False, 'limitRecordsDebug': '{{{SparkConfigurations.DEBUG_MOCK_DATA_LIMIT}}}', 'limitProcessingRecordsDebug': '{{{SparkConfigurations.DEBUG_PROCESSING_DATA_LIMIT}}}', 'doNotUseCacheData': True, 'unlimitedRecordsInProcessing': False, 'autoInferMaxFiles': '{{{SparkConfigurations.DEBUG_AUTO_INFER_MAX_FILES_LIMIT}}}', 'forceRunAsExecution': False, 'forceRunAsExecutionWithMaxSteps': '{{{SparkConfigurations.DEBUG_FORCE_RUN_AS_EXECUTION_WITH_MAX_STEPS}}}', 'executeWithSameExecutionMode': False, 'numberOfColumnExamples': '{{{SparkConfigurations.DEBUG_NUMBER_OF_COLUMN_EXAMPLES}}}', 'maxSizeColumnExamples': '{{{SparkConfigurations.DEBUG_MAX_SIZE_COLUMN_EXAMPLES}}}', 'executeDataAnalysisInAllSteps': True}, 'autoDebugSettings': {'enableAutoDebug': True, 'forceAutoDebugExecutionForAllSteps': False, 'doNotUseCacheData': True}, 'parametersSettings': {'userDefinedParameters': []}, 'getTotalRowsByStep': False, 'enableProjectEnvVar': True, 'executionMetricsSettings': {'customMetricLabels': []}}, 'streamingSettings': {'window': '{{{SparkConfigurations.SPARK_STREAMING_WINDOW}}}', 'backpressure': False, 'blockInterval': '{{{SparkConfigurations.SPARK_STREAMING_BLOCK_INTERVAL}}}', 'stopGracefully': True, 'checkpointSettings': {'checkpointPath': '{{{SparkConfigurations.SPARK_STREAMING_CHECKPOINT_PATH}}}', 'enableCheckpointing': True, 'autoDeleteCheckpoint': True, 'addTimeToCheckpointPath': False, 'keepSameCheckpoint': False}}, 'sparkSettings': {'sparkKerberos': True, 'sparkDataStoreTls': True, 'sparkVaultSecretList': False, 'sparkVaultSecretListNames': [], 'sparkConf': {'sparkResourcesConf': {'executorMemory': '{{{SparkResources.SPARK_EXECUTOR_MEMORY}}}', 'executorCores': '{{{SparkResources.SPARK_EXECUTOR_CORES}}}', 'driverCores': '{{{SparkResources.SPARK_DRIVER_CORES}}}', 'driverMemory': '{{{SparkResources.SPARK_DRIVER_MEMORY}}}', 'limitModeDriverCores': 'SOFT', 'limitModeDriverMemory': 'GUARANTEED', 'limitModeExecutorCores': 'SOFT', 'executorInstances': '{{{SparkResources.SPARK_EXECUTOR_INSTANCES}}}', 'enableDriverGpus': False, 'driverGpus': '{{{SparkResources.SPARK_DRIVER_GPUS}}}', 'enableExecutorGpus': False, 'executorGpus': '{{{SparkResources.SPARK_EXECUTOR_GPUS}}}'}, 'sparkHistoryServerConf': {'enableHistoryServerMonitoring': False, 'sparkHistoryServerEventLogRotateEnable': False, 'sparkHistoryServerEventLogRotateMaxFileSize': '{{{SparkConfigurations.SPARK_HISTORY_SERVER_EVENT_LOG_ROTATE_MAX_FILE_SIZE}}}'}, 'userSparkConf': [], 'sparkUser': 'root', 'logStagesProgress': False, 'hdfsTokenCache': True, 'executorExtraJavaOptions': '{{{SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS}}}', 'stopGracefullyTimeout': '{{{SparkResources.SPARK_KUBERNETES_SHUTDOWN}}}', 'sparkSchedulingConf': {'minRegisteredResourcesRatio': '{{{SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO}}}', 'maxRegisteredResourcesWaitingTime': '{{{SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME}}}'}, 'sparkMetricsConf': {'sparkMetricsEnabled': False, 'sparkDriverSourcesWhitelist': '{{{SparkConfigurations.SPARK_DRIVER_METRICS_SOURCES_WHITELIST}}}', 'sparkDriverUnregisteredMetrics': [], 'sparkExecutorSourcesWhitelist': '{{{SparkConfigurations.SPARK_EXECUTOR_METRICS_SOURCES_WHITELIST}}}', 'sparkExecutorUnregisteredMetrics': []}, 'enableProjectSparkConf': True}}, 'errorsManagement': {'genericErrorManagement': {'whenError': 'Error'}}, 'pythonEnvDefinition': {'vEnvManagementMode': 'DefaultExecutionVirtualEnv', 'condaYamlDefinition': 'name: rocket-default\n\nchannels:\n  - conda-forge\n  - nodefaults\n\ndependencies:\n  - python=3.9.*\n  - pip=25.1.*\n  - pip:\n      - mlflow==2.18.*\n      - pyarrow==14.*\n      - scikit-learn==1.*\n      - numpy==1.23.*\n      - scipy==1.*\n      - pandas==1.*\n      - petastorm==0.12.*\n      - langchain==0.3.*\n      - tiktoken==0.9.0', 'freezeAfterDebug': False, 'condaPackExtension': [], 'executeCondaUnpackAfterActivate': False, 'pySparkNativeExtensions': []}, 'structuredStreamingSettings': {}},
    raw_ui_settings={'position': {'x': 0.0, 'y': 0.0, 'k': 1.0}},
    raw_metadata={'group': {'id': '4e71db55-7545-4bbf-958e-46247706885f', 'name': '/home/system/workspace'}, 'groupId': '4e71db55-7545-4bbf-958e-46247706885f', 'projectId': '7gj83a6q-t894-20ld2-45et-a4o433g4h6d1', 'versionSparta': '3.6.1', 'creationDate': '2026-02-11T01:07:49Z', 'lastUpdateDate': '2026-02-11T01:09:40Z', 'version': 0, 'readOnly': False, 'releaseInProgress': False, 'tags': [], 'debugMode': False, 'debugAsExecutionMaybe': False, 'normalizedName': 'demo', 'isHybridStreaming': False, 'workflowType': 'SpartaWorkflow', 'workflowMasterId': '5c1d1353-a2b7-4067-88e1-0bce2c5adbe8'},
    annotations=[],
    node_groups=[],
    raw_nodes_order=['Csv', 'F_Datos', 'Transformacion', 'Print'],
    raw_edges_order=[{'origin': 'Csv', 'destination': 'F_Datos', 'dataType': 'ValidData'}, {'origin': 'F_Datos', 'destination': 'Transformacion', 'dataType': 'ValidData'}, {'origin': 'Transformacion', 'destination': 'Print', 'dataType': 'ValidData'}],
    skip_validation=True
)
def workflow():
    """
    Workflow importado desde JSON de Rocket.
    """
    # Input nodes
    csv_step = csv(
        name="Csv",
        path="/user/rocket.stratio-rocket/practica_episodio.csv",
        header=True,
        delimiter=",",
        priority=50
    )
    csv_step.node.configuration = {'schema.inputMode': 'NOSCHEMAPROVIDED', 'excludeGlobFilter': '', 'inputOptions': '', 'priority': '50', 'path': '/user/rocket.stratio-rocket/practica_episodio.csv', 'subdirGlobFilter': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'isSaved': True, 'subdirRegexFilter': '', 'readMode': 'DefaultReadMode', 'excludeRegexFilter': '', 'header': True, 'genAIMetadataColumns': '', 'delimiter': ','}
    csv_step.node.priority = 50
    csv_step.node.step_type = StepType.INPUT
    csv_step.node.class_name = "CsvInputStep"
    csv_step.node.class_pretty_name = "Csv"
    csv_step.node.supported_engines = ['Batch', 'Hybrid']
    csv_step.node.supported_data_relations = ['ValidData']
    csv_step.node.execution_engine = ExecutionEngine.HYBRID
    csv_step.node.arity = ['NullaryToNary']
    csv_step.node.ui_configuration = {'position': {'x': 271.0, 'y': 260.0}}
    csv_step.node.last_modified = "2026-02-11T01:10:10Z"
    csv_step.node.include_debug_options = True
    csv_step.node.include_supported_data_relations = True
    csv_step.node.include_description = True

    # Transformation nodes
    f_datos = filter(
        name="F_Datos",
        quote_sql=False,
        filter_exp="id < 100",
        inputs=csv_step,
        priority=50
    )
    f_datos.node.configuration = {'quoteSql': False, 'priority': '50', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'inputSchemas': '', 'filterExp': 'id < 100', 'genAIMetadataColumns': ''}
    f_datos.node.priority = 50
    f_datos.node.step_type = StepType.TRANSFORMATION
    f_datos.node.class_name = "FilterTransformStep"
    f_datos.node.class_pretty_name = "Filter"
    f_datos.node.supported_engines = ['Streaming', 'Batch', 'Hybrid']
    f_datos.node.supported_data_relations = ['ValidData', 'DiscardedData']
    f_datos.node.execution_engine = ExecutionEngine.HYBRID
    f_datos.node.arity = ['UnaryToNary']
    f_datos.node.ui_configuration = {'position': {'x': 454.0, 'y': 258.0}}
    f_datos.node.last_modified = "2026-02-11T01:10:36Z"
    f_datos.node.include_debug_options = True
    f_datos.node.include_supported_data_relations = True
    f_datos.node.include_description = True
    transformacion = trigger(
        name="Transformacion",
        sql="""
SELECT *
FROM F_Datos
""",
        quote_sql=False,
        replace_with_input_dataframe=False,
        inputs=f_datos,
        priority=50
    )
    transformacion.node.configuration = {'sql': 'SELECT *\r\nFROM F_Datos', 'quoteSql': False, 'priority': '50', 'discardConditions': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'replaceWithInputDataframe': False, 'genAIMetadataColumns': ''}
    transformacion.node.priority = 50
    transformacion.node.step_type = StepType.TRANSFORMATION
    transformacion.node.class_name = "TriggerTransformStep"
    transformacion.node.class_pretty_name = "Trigger"
    transformacion.node.supported_engines = ['Hybrid']
    transformacion.node.supported_data_relations = ['ValidData', 'DiscardedData']
    transformacion.node.execution_engine = ExecutionEngine.HYBRID
    transformacion.node.arity = ['NaryToNary']
    transformacion.node.ui_configuration = {'position': {'x': 641.0, 'y': 262.0}}
    transformacion.node.last_modified = "2026-02-11T01:10:31Z"
    transformacion.node.include_debug_options = True
    transformacion.node.include_supported_data_relations = True
    transformacion.node.include_description = True
    transformacion.node.outputs_writer = [{'saveMode': 'Append', 'outputStepName': 'Print', 'tableName': '', 'discardTableName': '', 'extraOptions': {'checkIfEmpty': False}}]

    # Output nodes
    print = print_step(
        name="Print",
        print_data=False,
        print_schema=False,
        print_metadata=True,
        log_level="warn",
        inputs=transformacion,
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
    print.node.ui_configuration = {'position': {'x': 813.0, 'y': 261.0}}
    print.node.last_modified = "2026-02-11T01:10:45Z"
    print.node.include_debug_options = True
    print.node.include_supported_data_relations = False
    print.node.include_description = False

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "exported_rebuilt.json")
