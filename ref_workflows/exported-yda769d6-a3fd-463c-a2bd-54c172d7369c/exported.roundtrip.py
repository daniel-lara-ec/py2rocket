"""
Workflow generado desde JSON de Rocket

Workflow: demo
ID: dda769d6-a3fd-463c-a2bd-54c172d7369c
"""

from py2rocket import pipeline, build
from py2rocket.core.input import csv
from py2rocket.core.output import parquet_output
from py2rocket.core.pipeline import ExecutionEngine, StepType
from py2rocket.core.transformation import filter
from py2rocket.core.transformation import trigger
from py2rocket.core.transformation import union

@pipeline(
    name="demo",
    execution_engine="Hybrid",
    workflow_id="dda769d6-a3fd-463c-a2bd-54c172d7369c",
    project_id='7gj83a6q-t894-20ld2-45et-a4o433g4h6d1',
    group_id='4e71db55-7545-4bbf-958e-46247706885f',
    asset_id="5c1d1353-a2b7-4067-88e1-0bce2c5adbe8",
    parameters_lists=['Environment', 'SparkResources', 'SparkConfigurations'],
    raw_settings={'global': {'executionMode': 'kubernetes', 'dockerSettings': {'driverDockerImage': '{{{SparkConfigurations.SPARK_DRIVER_DOCKER_IMAGE}}}', 'driverDockerVolumes': '{{{SparkConfigurations.SPARK_DRIVER_DOCKER_VOLUMES}}}', 'executorDockerImage': '{{{SparkConfigurations.SPARK_EXECUTOR_DOCKER_IMAGE}}}', 'executorDockerVolumes': '{{{SparkConfigurations.SPARK_EXECUTOR_DOCKER_VOLUMES}}}'}, 'userPluginsJars': [], 'parametersLists': ['Environment', 'SparkResources', 'SparkConfigurations'], 'parametersUsed': ['SparkConfigurations.DEBUG_AUTO_INFER_MAX_FILES_LIMIT', 'SparkConfigurations.DEBUG_MAX_SIZE_COLUMN_EXAMPLES', 'SparkConfigurations.DEBUG_MOCK_DATA_LIMIT', 'SparkConfigurations.DEBUG_NUMBER_OF_COLUMN_EXAMPLES', 'SparkConfigurations.DEBUG_PROCESSING_DATA_LIMIT', 'SparkConfigurations.HEALTH_CHECK_FAILURES_TIMEOUT', 'SparkConfigurations.HEALTH_CHECK_GRACE_PERIOD_SECONDS', 'SparkConfigurations.HEALTH_CHECK_INTERVAL_SECONDS', 'SparkConfigurations.HEALTH_CHECK_MAX_CONSECUTIVE_FAILURES', 'SparkConfigurations.SPARK_DRIVER_DOCKER_IMAGE', 'SparkConfigurations.SPARK_DRIVER_DOCKER_VOLUMES', 'SparkConfigurations.SPARK_DRIVER_METRICS_SOURCES_WHITELIST', 'SparkConfigurations.SPARK_EXECUTOR_DOCKER_IMAGE', 'SparkConfigurations.SPARK_EXECUTOR_DOCKER_VOLUMES', 'SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS', 'SparkConfigurations.SPARK_EXECUTOR_METRICS_SOURCES_WHITELIST', 'SparkConfigurations.SPARK_HISTORY_SERVER_EVENT_LOG_ROTATE_MAX_FILE_SIZE', 'SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME', 'SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO', 'SparkResources.SPARK_DRIVER_CORES', 'SparkResources.SPARK_DRIVER_GPUS', 'SparkResources.SPARK_DRIVER_MEMORY', 'SparkResources.SPARK_EXECUTOR_CORES', 'SparkResources.SPARK_EXECUTOR_GPUS', 'SparkResources.SPARK_EXECUTOR_INSTANCES', 'SparkResources.SPARK_EXECUTOR_MEMORY', 'SparkResources.SPARK_KUBERNETES_SHUTDOWN'], 'sqlSettings': {'preExecutionSqlSentences': [], 'postExecutionSqlSentences': [], 'udfsToRegister': [], 'udafsToRegister': []}, 'kubernetesDeploymentSettings': {'gracePeriodSeconds': '{{{SparkConfigurations.HEALTH_CHECK_GRACE_PERIOD_SECONDS}}}', 'intervalSeconds': '{{{SparkConfigurations.HEALTH_CHECK_INTERVAL_SECONDS}}}', 'timeoutSeconds': '{{{SparkConfigurations.HEALTH_CHECK_FAILURES_TIMEOUT}}}', 'maxConsecutiveFailures': '{{{SparkConfigurations.HEALTH_CHECK_MAX_CONSECUTIVE_FAILURES}}}', 'imagePullPolicy': 'IfNotPresent', 'userEnvVariables': [], 'userLabels': [], 'includePostgresHealthCheck': True, 'includeHdfsHealthCheck': True, 'includeSparkHealthCheck': True, 'driverPlacements': {'addedPlacements': [], 'configurableProjectPlacementsEnabled': True}, 'executorPlacements': {'addedPlacements': [], 'configurableProjectPlacementsEnabled': True}, 'driverVolumes': {'addedVolumes': {}, 'excludedVolumes': []}, 'executorVolumes': {'addedVolumes': {}, 'excludedVolumes': []}}, 'enableQualityRules': True, 'debugSettings': {'forceDebugExecutionForAllSteps': False, 'limitRecordsDebug': '{{{SparkConfigurations.DEBUG_MOCK_DATA_LIMIT}}}', 'limitProcessingRecordsDebug': '{{{SparkConfigurations.DEBUG_PROCESSING_DATA_LIMIT}}}', 'doNotUseCacheData': True, 'unlimitedRecordsInProcessing': False, 'autoInferMaxFiles': '{{{SparkConfigurations.DEBUG_AUTO_INFER_MAX_FILES_LIMIT}}}', 'forceRunAsExecution': False, 'forceRunAsExecutionWithMaxSteps': '20', 'executeWithSameExecutionMode': False, 'numberOfColumnExamples': '{{{SparkConfigurations.DEBUG_NUMBER_OF_COLUMN_EXAMPLES}}}', 'maxSizeColumnExamples': '{{{SparkConfigurations.DEBUG_MAX_SIZE_COLUMN_EXAMPLES}}}', 'executeDataAnalysisInAllSteps': True}, 'autoDebugSettings': {'enableAutoDebug': True, 'forceAutoDebugExecutionForAllSteps': False, 'doNotUseCacheData': True}, 'parametersSettings': {'userDefinedParameters': []}, 'getTotalRowsByStep': False, 'enableProjectEnvVar': True, 'executionMetricsSettings': {'customMetricLabels': []}}, 'streamingSettings': {'window': '2s', 'backpressure': False, 'blockInterval': '100ms', 'stopGracefully': True, 'checkpointSettings': {'checkpointPath': 'tmp/checkpoint', 'enableCheckpointing': True, 'autoDeleteCheckpoint': True, 'addTimeToCheckpointPath': False, 'keepSameCheckpoint': False}}, 'sparkSettings': {'sparkKerberos': True, 'sparkDataStoreTls': True, 'sparkVaultSecretList': False, 'sparkVaultSecretListNames': [], 'sparkConf': {'sparkResourcesConf': {'executorMemory': '{{{SparkResources.SPARK_EXECUTOR_MEMORY}}}', 'executorCores': '{{{SparkResources.SPARK_EXECUTOR_CORES}}}', 'driverCores': '{{{SparkResources.SPARK_DRIVER_CORES}}}', 'driverMemory': '{{{SparkResources.SPARK_DRIVER_MEMORY}}}', 'limitModeDriverCores': 'SOFT', 'limitModeDriverMemory': 'GUARANTEED', 'limitModeExecutorCores': 'SOFT', 'executorInstances': '{{{SparkResources.SPARK_EXECUTOR_INSTANCES}}}', 'enableDriverGpus': False, 'driverGpus': '{{{SparkResources.SPARK_DRIVER_GPUS}}}', 'enableExecutorGpus': False, 'executorGpus': '{{{SparkResources.SPARK_EXECUTOR_GPUS}}}'}, 'sparkHistoryServerConf': {'enableHistoryServerMonitoring': False, 'sparkHistoryServerEventLogRotateEnable': False, 'sparkHistoryServerEventLogRotateMaxFileSize': '{{{SparkConfigurations.SPARK_HISTORY_SERVER_EVENT_LOG_ROTATE_MAX_FILE_SIZE}}}'}, 'userSparkConf': [], 'sparkUser': 'root', 'logStagesProgress': False, 'hdfsTokenCache': True, 'executorExtraJavaOptions': '{{{SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS}}}', 'stopGracefullyTimeout': '{{{SparkResources.SPARK_KUBERNETES_SHUTDOWN}}}', 'sparkSchedulingConf': {'minRegisteredResourcesRatio': '{{{SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO}}}', 'maxRegisteredResourcesWaitingTime': '{{{SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME}}}'}, 'sparkMetricsConf': {'sparkMetricsEnabled': False, 'sparkDriverSourcesWhitelist': '{{{SparkConfigurations.SPARK_DRIVER_METRICS_SOURCES_WHITELIST}}}', 'sparkDriverUnregisteredMetrics': [], 'sparkExecutorSourcesWhitelist': '{{{SparkConfigurations.SPARK_EXECUTOR_METRICS_SOURCES_WHITELIST}}}', 'sparkExecutorUnregisteredMetrics': []}, 'enableProjectSparkConf': True}}, 'errorsManagement': {'genericErrorManagement': {'whenError': 'Error'}}, 'pythonEnvDefinition': {'vEnvManagementMode': 'CondaYamlDescriptor', 'condaYamlDefinition': 'name: rocket-default\n\nchannels:\n  - conda-forge\n  - nodefaults\n\ndependencies:\n  - python=3.9.*\n  - pip=25.1.*\n  - pip:\n      - mlflow==2.18.*\n      - pyarrow==14.*\n      - scikit-learn==1.*\n      - numpy==1.23.*\n      - scipy==1.*\n      - pandas==1.*\n      - petastorm==0.12.*\n      - langchain==0.3.*\n      - tiktoken==0.9.0\n      - geopandas', 'freezeAfterDebug': False, 'frozenCondaYaml': '', 'condaEnvironmentIdentifier': '', 'condaPackEnvironmentPath': '', 'condaPackExtension': [], 'executeCondaUnpackAfterActivate': False, 'pySparkNativeExtensions': []}, 'structuredStreamingSettings': {}},
    raw_ui_settings={'position': {'x': 45.30365831624613, 'y': 152.6320574523963, 'k': 1.0421205719080704}},
    raw_metadata={'group': {'id': '4e71db55-7545-4bbf-958e-46247706885f', 'name': '/home/system/workspace'}, 'groupId': '4e71db55-7545-4bbf-958e-46247706885f', 'projectId': '7gj83a6q-t894-20ld2-45et-a4o433g4h6d1', 'versionSparta': '3.6.1', 'creationDate': '2026-02-11T01:07:49Z', 'lastUpdateDate': '2026-02-11T01:27:13Z', 'version': 0, 'readOnly': False, 'releaseInProgress': False, 'tags': [], 'debugMode': False, 'debugAsExecutionMaybe': False, 'normalizedName': 'demo', 'isHybridStreaming': False, 'workflowType': 'SpartaWorkflow', 'workflowMasterId': '5c1d1353-a2b7-4067-88e1-0bce2c5adbe8'},
    annotations=[],
    node_groups=[],
    raw_nodes_order=['Csv', 'F_Datos', 'Transformacion', 'F_Datos2', 'UnionDatos', 'Parquet'],
    raw_edges_order=[{'origin': 'Csv', 'destination': 'F_Datos', 'dataType': 'ValidData'}, {'origin': 'Csv', 'destination': 'F_Datos2', 'dataType': 'ValidData'}, {'origin': 'F_Datos', 'destination': 'UnionDatos', 'dataType': 'DiscardedData'}, {'origin': 'F_Datos2', 'destination': 'UnionDatos', 'dataType': 'ValidData'}, {'origin': 'UnionDatos', 'destination': 'Transformacion', 'dataType': 'ValidData'}, {'origin': 'Transformacion', 'destination': 'Parquet', 'dataType': 'ValidData'}],
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
    csv_step.node.ui_configuration = {'position': {'x': 65.05086517333984, 'y': 170.35191345214844}}
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
    f_datos.node.ui_configuration = {'position': {'x': 310.9101257324219, 'y': 74.39373016357422}}
    f_datos.node.last_modified = "2026-02-11T01:10:36Z"
    f_datos.node.include_debug_options = True
    f_datos.node.include_supported_data_relations = True
    f_datos.node.include_description = True
    f_datos2 = filter(
        name="F_Datos2",
        quote_sql=False,
        filter_exp="id >= 200",
        inputs=csv_step,
        priority=50
    )
    f_datos2.node.configuration = {'quoteSql': False, 'priority': '50', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'inputSchemas': '', 'filterExp': 'id >= 200', 'genAIMetadataColumns': ''}
    f_datos2.node.priority = 50
    f_datos2.node.step_type = StepType.TRANSFORMATION
    f_datos2.node.class_name = "FilterTransformStep"
    f_datos2.node.class_pretty_name = "Filter"
    f_datos2.node.supported_engines = ['Streaming', 'Batch', 'Hybrid']
    f_datos2.node.supported_data_relations = ['ValidData', 'DiscardedData']
    f_datos2.node.execution_engine = ExecutionEngine.HYBRID
    f_datos2.node.arity = ['UnaryToNary']
    f_datos2.node.ui_configuration = {'position': {'x': 310.46190990595494, 'y': 192.7753577036979}}
    f_datos2.node.last_modified = "2026-02-11T01:24:59Z"
    f_datos2.node.include_debug_options = True
    f_datos2.node.include_supported_data_relations = True
    f_datos2.node.include_description = True
    uniondatos = union(
        name="UnionDatos",
        inputs=[f_datos.discarded, f_datos2],
        priority=50
    )
    uniondatos.node.configuration = {'priority': '50', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'inputSchemas': '', 'genAIMetadataColumns': ''}
    uniondatos.node.priority = 50
    uniondatos.node.step_type = StepType.TRANSFORMATION
    uniondatos.node.class_name = "UnionTransformStep"
    uniondatos.node.class_pretty_name = "Union"
    uniondatos.node.supported_engines = ['Streaming', 'Batch', 'Hybrid']
    uniondatos.node.supported_data_relations = ['ValidData']
    uniondatos.node.execution_engine = ExecutionEngine.HYBRID
    uniondatos.node.arity = ['NaryToNary']
    uniondatos.node.ui_configuration = {'position': {'x': 496.7720422699663, 'y': 129.89663183509563}}
    uniondatos.node.last_modified = "2026-02-11T01:25:05Z"
    uniondatos.node.include_debug_options = True
    uniondatos.node.include_supported_data_relations = True
    uniondatos.node.include_description = True
    transformacion = trigger(
        name="Transformacion",
        sql="""
SELECT *
FROM UnionDatos
""",
        quote_sql=False,
        replace_with_input_dataframe=False,
        inputs=uniondatos,
        priority=50
    )
    transformacion.node.configuration = {'sql': 'SELECT *\r\nFROM UnionDatos', 'quoteSql': False, 'priority': '50', 'discardConditions': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'replaceWithInputDataframe': False, 'genAIMetadataColumns': ''}
    transformacion.node.priority = 50
    transformacion.node.step_type = StepType.TRANSFORMATION
    transformacion.node.class_name = "TriggerTransformStep"
    transformacion.node.class_pretty_name = "Trigger"
    transformacion.node.supported_engines = ['Hybrid']
    transformacion.node.supported_data_relations = ['ValidData', 'DiscardedData']
    transformacion.node.execution_engine = ExecutionEngine.HYBRID
    transformacion.node.arity = ['NaryToNary']
    transformacion.node.ui_configuration = {'position': {'x': 665.2021484375, 'y': 131.00904083251953}}
    transformacion.node.last_modified = "2026-02-11T01:25:15Z"
    transformacion.node.include_debug_options = True
    transformacion.node.include_supported_data_relations = True
    transformacion.node.include_description = True
    transformacion.node.outputs_writer = [{'saveMode': 'Overwrite', 'outputStepName': 'Parquet', 'tableName': 'TABLA', 'discardTableName': '', 'extraOptions': {'checkIfEmpty': True, 'partitionBy': 'tipo', 'partitionOverwriteEnabled': True, 'partitionColumns': '', 'saveMode': 'Overwrite', 'partitions': ''}}]

    # Output nodes
    parquet = parquet_output(
        name="Parquet",
        path="/user/data/save",
        inputs=transformacion,
        priority=50
    )
    parquet.node.configuration = {'path': '/user/data/save', 'saveOptions': '', 'isSaved': True, 'priority': '50'}
    parquet.node.priority = 50
    parquet.node.step_type = StepType.OUTPUT
    parquet.node.class_name = "ParquetOutputStep"
    parquet.node.class_pretty_name = "Parquet"
    parquet.node.supported_engines = ['Streaming', 'Batch', 'Hybrid']
    parquet.node.execution_engine = ExecutionEngine.HYBRID
    parquet.node.arity = ['NullaryToNullary', 'NaryToNullary']
    parquet.node.ui_configuration = {'position': {'x': 829.7469137331135, 'y': 131.81577400313077}}
    parquet.node.last_modified = "2026-02-11T01:27:17Z"
    parquet.node.include_debug_options = False
    parquet.node.include_supported_data_relations = False
    parquet.node.include_description = True

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "exported_rebuilt.json")
