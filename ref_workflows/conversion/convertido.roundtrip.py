"""
Workflow generado desde JSON de Rocket

Workflow: pl-transformacion-Zp-Mdp-capa
ID: 67d9dbbc-3d7b-4611-ba2f-aaefdb431a10
"""

from py2rocket import pipeline, build
from py2rocket.core.operations import filter
from py2rocket.core.operations import parquet
from py2rocket.core.operations import parquet_output
from py2rocket.core.operations import sql
from py2rocket.core.operations import trigger

@pipeline(
    name="pl-transformacion-Zp-Mdp-capa",
    execution_engine="Hybrid",
    workflow_id="67d9dbbc-3d7b-4611-ba2f-aaefdb431a10",
    asset_id="3d3d44bf-96bd-4f65-b731-44f14fecdbb9",
    parameters_lists=['Environment', 'SparkResources', 'SparkConfigurations', 'ParamsRecepcionRemesas'],
    raw_settings={'global': {'executionMode': 'kubernetes', 'dockerSettings': {'driverDockerImage': '{{{SparkConfigurations.SPARK_DRIVER_DOCKER_IMAGE}}}', 'driverDockerVolumes': '{{{SparkConfigurations.SPARK_DRIVER_DOCKER_VOLUMES}}}', 'executorDockerImage': '{{{SparkConfigurations.SPARK_EXECUTOR_DOCKER_IMAGE}}}', 'executorDockerVolumes': '{{{SparkConfigurations.SPARK_EXECUTOR_DOCKER_VOLUMES}}}'}, 'userPluginsJars': [], 'parametersLists': ['Environment', 'SparkResources', 'SparkConfigurations', 'ParamsRecepcionRemesas'], 'parametersUsed': ['SparkConfigurations.DEBUG_AUTO_INFER_MAX_FILES_LIMIT', 'SparkConfigurations.DEBUG_FORCE_RUN_AS_EXECUTION_WITH_MAX_STEPS', 'SparkConfigurations.DEBUG_MAX_SIZE_COLUMN_EXAMPLES', 'SparkConfigurations.DEBUG_MOCK_DATA_LIMIT', 'SparkConfigurations.DEBUG_NUMBER_OF_COLUMN_EXAMPLES', 'SparkConfigurations.DEBUG_PROCESSING_DATA_LIMIT', 'SparkConfigurations.HEALTH_CHECK_FAILURES_TIMEOUT', 'SparkConfigurations.HEALTH_CHECK_GRACE_PERIOD_SECONDS', 'SparkConfigurations.HEALTH_CHECK_INTERVAL_SECONDS', 'SparkConfigurations.HEALTH_CHECK_MAX_CONSECUTIVE_FAILURES', 'SparkConfigurations.SPARK_DRIVER_DOCKER_IMAGE', 'SparkConfigurations.SPARK_DRIVER_DOCKER_VOLUMES', 'SparkConfigurations.SPARK_EXECUTOR_DOCKER_IMAGE', 'SparkConfigurations.SPARK_EXECUTOR_DOCKER_VOLUMES', 'SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS', 'SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME', 'SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO', 'SparkResources.SPARK_DRIVER_CORES', 'SparkResources.SPARK_DRIVER_MEMORY', 'SparkResources.SPARK_EXECUTOR_CORES', 'SparkResources.SPARK_EXECUTOR_INSTANCES', 'SparkResources.SPARK_EXECUTOR_MEMORY', 'SparkResources.SPARK_KUBERNETES_SHUTDOWN'], 'sqlSettings': {'preExecutionSqlSentences': [], 'postExecutionSqlSentences': [], 'udfsToRegister': [], 'udafsToRegister': []}, 'kubernetesDeploymentSettings': {'gracePeriodSeconds': '{{{SparkConfigurations.HEALTH_CHECK_GRACE_PERIOD_SECONDS}}}', 'intervalSeconds': '{{{SparkConfigurations.HEALTH_CHECK_INTERVAL_SECONDS}}}', 'timeoutSeconds': '{{{SparkConfigurations.HEALTH_CHECK_FAILURES_TIMEOUT}}}', 'maxConsecutiveFailures': '{{{SparkConfigurations.HEALTH_CHECK_MAX_CONSECUTIVE_FAILURES}}}', 'imagePullPolicy': 'IfNotPresent', 'userEnvVariables': [], 'userLabels': [], 'includePostgresHealthCheck': True, 'includeHdfsHealthCheck': True, 'includeSparkHealthCheck': True, 'driverPlacements': {'addedPlacements': [], 'configurableProjectPlacementsEnabled': True}, 'executorPlacements': {'addedPlacements': [], 'configurableProjectPlacementsEnabled': True}, 'driverVolumes': {'addedVolumes': {}, 'excludedVolumes': []}, 'executorVolumes': {'addedVolumes': {}, 'excludedVolumes': []}}, 'enableQualityRules': True, 'debugSettings': {'forceDebugExecutionForAllSteps': False, 'limitRecordsDebug': '{{{SparkConfigurations.DEBUG_MOCK_DATA_LIMIT}}}', 'limitProcessingRecordsDebug': '{{{SparkConfigurations.DEBUG_PROCESSING_DATA_LIMIT}}}', 'doNotUseCacheData': True, 'unlimitedRecordsInProcessing': False, 'autoInferMaxFiles': '{{{SparkConfigurations.DEBUG_AUTO_INFER_MAX_FILES_LIMIT}}}', 'forceRunAsExecution': False, 'forceRunAsExecutionWithMaxSteps': '{{{SparkConfigurations.DEBUG_FORCE_RUN_AS_EXECUTION_WITH_MAX_STEPS}}}', 'executeWithSameExecutionMode': False, 'numberOfColumnExamples': '{{{SparkConfigurations.DEBUG_NUMBER_OF_COLUMN_EXAMPLES}}}', 'maxSizeColumnExamples': '{{{SparkConfigurations.DEBUG_MAX_SIZE_COLUMN_EXAMPLES}}}', 'executeDataAnalysisInAllSteps': True}, 'autoDebugSettings': {'enableAutoDebug': True, 'forceAutoDebugExecutionForAllSteps': False, 'doNotUseCacheData': True}, 'parametersSettings': {'userDefinedParameters': []}, 'getTotalRowsByStep': False, 'enableProjectEnvVar': True, 'executionMetricsSettings': {'customMetricLabels': []}}, 'streamingSettings': {'window': '{{{SparkConfigurations.SPARK_STREAMING_WINDOW}}}', 'backpressure': False, 'blockInterval': '{{{SparkConfigurations.SPARK_STREAMING_BLOCK_INTERVAL}}}', 'stopGracefully': True, 'checkpointSettings': {'checkpointPath': '{{{SparkConfigurations.SPARK_STREAMING_CHECKPOINT_PATH}}}', 'enableCheckpointing': True, 'autoDeleteCheckpoint': True, 'addTimeToCheckpointPath': False, 'keepSameCheckpoint': False}}, 'sparkSettings': {'sparkKerberos': True, 'sparkDataStoreTls': True, 'sparkVaultSecretList': False, 'sparkVaultSecretListNames': [], 'sparkConf': {'sparkResourcesConf': {'executorMemory': '{{{SparkResources.SPARK_EXECUTOR_MEMORY}}}', 'executorCores': '{{{SparkResources.SPARK_EXECUTOR_CORES}}}', 'driverCores': '{{{SparkResources.SPARK_DRIVER_CORES}}}', 'driverMemory': '{{{SparkResources.SPARK_DRIVER_MEMORY}}}', 'limitModeDriverCores': 'SOFT', 'limitModeDriverMemory': 'GUARANTEED', 'limitModeExecutorCores': 'SOFT', 'executorInstances': '{{{SparkResources.SPARK_EXECUTOR_INSTANCES}}}', 'enableDriverGpus': False, 'driverGpus': '{{{SparkResources.SPARK_DRIVER_GPUS}}}', 'enableExecutorGpus': False, 'executorGpus': '{{{SparkResources.SPARK_EXECUTOR_GPUS}}}'}, 'sparkHistoryServerConf': {'enableHistoryServerMonitoring': False, 'sparkHistoryServerEventLogRotateEnable': False, 'sparkHistoryServerEventLogRotateMaxFileSize': '{{{SparkConfigurations.SPARK_HISTORY_SERVER_EVENT_LOG_ROTATE_MAX_FILE_SIZE}}}'}, 'userSparkConf': [], 'sparkUser': 'root', 'logStagesProgress': False, 'hdfsTokenCache': True, 'executorExtraJavaOptions': '{{{SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS}}}', 'stopGracefullyTimeout': '{{{SparkResources.SPARK_KUBERNETES_SHUTDOWN}}}', 'sparkSchedulingConf': {'minRegisteredResourcesRatio': '{{{SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO}}}', 'maxRegisteredResourcesWaitingTime': '{{{SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME}}}'}, 'sparkMetricsConf': {'sparkMetricsEnabled': False, 'sparkDriverSourcesWhitelist': '{{{SparkConfigurations.SPARK_DRIVER_METRICS_SOURCES_WHITELIST}}}', 'sparkDriverUnregisteredMetrics': [], 'sparkExecutorSourcesWhitelist': '{{{SparkConfigurations.SPARK_EXECUTOR_METRICS_SOURCES_WHITELIST}}}', 'sparkExecutorUnregisteredMetrics': []}, 'enableProjectSparkConf': True}}, 'errorsManagement': {'genericErrorManagement': {'whenError': 'Error'}}, 'pythonEnvDefinition': {'vEnvManagementMode': 'DefaultExecutionVirtualEnv', 'condaYamlDefinition': 'name: rocket-default\n\nchannels:\n  - conda-forge\n  - nodefaults\n\ndependencies:\n  - python=3.9.*\n  - pip=25.1.*\n  - pip:\n      - mlflow==2.18.*\n      - pyarrow==14.*\n      - scikit-learn==1.*\n      - numpy==1.23.*\n      - scipy==1.*\n      - pandas==1.*\n      - petastorm==0.12.*\n      - langchain==0.3.*\n      - tiktoken==0.9.0', 'freezeAfterDebug': False, 'condaPackExtension': [], 'executeCondaUnpackAfterActivate': False, 'pySparkNativeExtensions': []}, 'structuredStreamingSettings': {}},
    raw_ui_settings={'position': {'x': -2083.536303142103, 'y': -859.7024044750958, 'k': 4.0}},
    raw_metadata={'versionSparta': '3.6.3', 'creationDate': '2026-02-12T21:21:30Z', 'lastUpdateDate': '2026-02-12T21:21:30Z', 'version': 0, 'readOnly': False, 'releaseInProgress': False, 'tags': [], 'debugMode': False, 'debugAsExecutionMaybe': False, 'normalizedName': 'pl-transformacion-zp-mdp-capa', 'isHybridStreaming': False, 'workflowType': 'SpartaWorkflow', 'workflowMasterId': '3d3d44bf-96bd-4f65-b731-44f14fecdbb9'},
    annotations=[],
    node_groups=[],
    raw_nodes_order=['Load_Catalogo_Cantones', 'Load_Emigracion', 'Load_Localizacion', 'Load_Poblacion', 'Load_ShareDigital', 'Pi_Geolocalizacion', 'SQL_DatosBasicos', 'F_LocalizacionNoEcuador', 'T_CruceDatosParr', 'F_PriorizacionParroquias', 'F_RegistroUnico', 'F_Trx', 'T_ClientesNoEcuador', 'T_CrucePotencialParroquia', 'T_FiltroTrxNoFisicas', 'T_OrigenInmigracion', 'Transformacion', 'Po_Guardado'],
    raw_edges_order=[{'origin': 'Pi_Geolocalizacion', 'destination': 'F_LocalizacionNoEcuador', 'dataType': 'ValidData'}, {'origin': 'Load_Catalogo_Cantones', 'destination': 'T_CruceDatosParr', 'dataType': 'ValidData'}, {'origin': 'Load_Poblacion', 'destination': 'T_CruceDatosParr', 'dataType': 'ValidData'}, {'origin': 'Load_Emigracion', 'destination': 'T_CruceDatosParr', 'dataType': 'ValidData'}, {'origin': 'T_CruceDatosParr', 'destination': 'F_PriorizacionParroquias', 'dataType': 'ValidData'}, {'origin': 'Load_Localizacion', 'destination': 'F_RegistroUnico', 'dataType': 'ValidData'}, {'origin': 'Load_ShareDigital', 'destination': 'F_Trx', 'dataType': 'ValidData'}, {'origin': 'SQL_DatosBasicos', 'destination': 'T_ClientesNoEcuador', 'dataType': 'ValidData'}, {'origin': 'F_LocalizacionNoEcuador', 'destination': 'T_ClientesNoEcuador', 'dataType': 'ValidData'}, {'origin': 'F_RegistroUnico', 'destination': 'T_CrucePotencialParroquia', 'dataType': 'ValidData'}, {'origin': 'F_PriorizacionParroquias', 'destination': 'T_CrucePotencialParroquia', 'dataType': 'ValidData'}, {'origin': 'T_ClientesNoEcuador', 'destination': 'T_FiltroTrxNoFisicas', 'dataType': 'ValidData'}, {'origin': 'F_Trx', 'destination': 'T_FiltroTrxNoFisicas', 'dataType': 'ValidData'}, {'origin': 'T_CrucePotencialParroquia', 'destination': 'T_OrigenInmigracion', 'dataType': 'ValidData'}, {'origin': 'T_FiltroTrxNoFisicas', 'destination': 'T_OrigenInmigracion', 'dataType': 'ValidData'}, {'origin': 'T_OrigenInmigracion', 'destination': 'Transformacion', 'dataType': 'ValidData'}, {'origin': 'Transformacion', 'destination': 'Po_Guardado', 'dataType': 'ValidData'}],
    skip_validation=True
)
def workflow():
    """
    Workflow importado desde JSON de Rocket.
    """
    # Input nodes
    load_catalogo_cantones = sql(
        name="Load_Catalogo_Cantones",
        query="""

SELECT DISTINCT dpaParroquia
FROM {{{TD_GEO_DPA_PARROQUIA}}}

""",
        force_native_query=False,
        cache_table=False,
        description='',
        priority=10,
        config_override={'debugOptions': '{"executeStepAutoDebug":true}', 'query': '\nSELECT DISTINCT dpaParroquia\nFROM {{{TD_GEO_DPA_PARROQUIA}}}\n', 'forceNativeQuery': False, 'cacheTable': False, 'isSaved': True, 'asyncRefresh': False, 'genAIMetadataTableDescription': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'SQLInputStep', 'class_pretty_name': 'SQL', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 612, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    load_emigracion = sql(
        name="Load_Emigracion",
        query="""

SELECT
    parroq AS dpaParroquia
    ,COUNT(1) AS conteoMigrantes
FROM {{{TD_CENSO_EMIGRACION}}}
WHERE e03 + (2025-e01) >= 18
AND e03 != 999
GROUP BY parroq

""",
        force_native_query=False,
        cache_table=False,
        description='',
        config_override={'debugOptions': '{"executeStepAutoDebug":true}', 'query': '\nSELECT\n    parroq AS dpaParroquia\n    ,COUNT(1) AS conteoMigrantes\nFROM {{{TD_CENSO_EMIGRACION}}}\nWHERE e03 + (2025-e01) >= 18\nAND e03 != 999\nGROUP BY parroq\n', 'forceNativeQuery': False, 'cacheTable': False, 'isSaved': True, 'asyncRefresh': False, 'genAIMetadataTableDescription': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'SQLInputStep', 'class_pretty_name': 'SQL', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 782, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    load_localizacion = sql(
        name="Load_Localizacion",
        query="""

SELECT
    codigoIdentificacionInternoCliente,
    provinciaDomicilioCliente AS provincia,
    codigoParroquiaDomicilioCliente,
    ROW_NUMBER() OVER (
        PARTITION BY
            codigoIdentificacionInternoCliente
        ORDER BY
            codigoParroquiaDomicilioCliente
    ) AS conteoDatos
FROM
    {{{PX_SDX}}}{{{TN_LOCALIZACION}}}
WHERE
    tipoDireccion = 'Domicilio'
    AND codigoPaisDomicilioCliente = 'EC'
    AND UPPER(TRIM(codigoProvinciaDomicilioCliente)) IN (
        '01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24'
    )

""",
        force_native_query=False,
        cache_table=False,
        description='',
        priority=100,
        config_override={'debugOptions': '{"executeStepAutoDebug":true}', 'query': "\nSELECT\n    codigoIdentificacionInternoCliente,\n    provinciaDomicilioCliente AS provincia,\n    codigoParroquiaDomicilioCliente,\n    ROW_NUMBER() OVER (\n        PARTITION BY\n            codigoIdentificacionInternoCliente\n        ORDER BY\n            codigoParroquiaDomicilioCliente\n    ) AS conteoDatos\nFROM\n    {{{PX_SDX}}}{{{TN_LOCALIZACION}}}\nWHERE\n    tipoDireccion = 'Domicilio'\n    AND codigoPaisDomicilioCliente = 'EC'\n    AND UPPER(TRIM(codigoProvinciaDomicilioCliente)) IN (\n        '01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24'\n    )\n", 'forceNativeQuery': False, 'cacheTable': False, 'isSaved': True, 'asyncRefresh': False, 'genAIMetadataTableDescription': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'SQLInputStep', 'class_pretty_name': 'SQL', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 952, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    load_poblacion = sql(
        name="Load_Poblacion",
        query="""

SELECT
    parroq AS dpaParroquia
    ,COUNT(1) AS conteoPoblacion
FROM {{{TD_CENSO_POBLACION}}}
WHERE p03 >= 18
GROUP BY parroq

""",
        force_native_query=False,
        cache_table=False,
        description='',
        config_override={'debugOptions': '{"executeStepAutoDebug":true}', 'query': '\nSELECT\n    parroq AS dpaParroquia\n    ,COUNT(1) AS conteoPoblacion\nFROM {{{TD_CENSO_POBLACION}}}\nWHERE p03 >= 18\nGROUP BY parroq\n', 'forceNativeQuery': False, 'cacheTable': False, 'isSaved': True, 'asyncRefresh': False, 'genAIMetadataTableDescription': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'SQLInputStep', 'class_pretty_name': 'SQL', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 1122, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    load_sharedigital = sql(
        name="Load_ShareDigital",
        query="""

SELECT
    codigoIdentificacionInternoCliente
    ,share_dig
    ,periodo
FROM dsc_medios_pago.ZP_BP_Mdp_TM_CapaShareDigital

""",
        force_native_query=False,
        cache_table=False,
        description='',
        config_override={'debugOptions': '{"executeStepAutoDebug":true}', 'query': '\nSELECT\n    codigoIdentificacionInternoCliente\n    ,share_dig\n    ,periodo\nFROM dsc_medios_pago.ZP_BP_Mdp_TM_CapaShareDigital\n', 'forceNativeQuery': False, 'cacheTable': False, 'isSaved': True, 'asyncRefresh': False, 'genAIMetadataTableDescription': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'SQLInputStep', 'class_pretty_name': 'SQL', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 1292, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    pi_geolocalizacion = parquet(
        name="Pi_Geolocalizacion",
        path="""

s3a://s3-lagodatos-noprod-04/data/tmp/analitica/20014/layerRemesasGeolocalizacionProvEcu

""",
        is_recursive_enabled=True,
        paths=[{'path': None, 'subdirGlobFilter': None, 'subdirRegexFilter': None, 'excludeGlobFilter': None, 'excludeRegexFilter': None}],
        metadata_column_enabled=True,
        enable_filter_pattern=True,
        path_glob_filter="*.parquet",
        description='',
        config_override={'debugOptions': {'executeStepAutoDebug': True, 'executeStepDebug': True, 'mockType': 'AutoInfer'}, 'path': '\ns3a://s3-lagodatos-noprod-04/data/tmp/analitica/20014/layerRemesasGeolocalizacionProvEcu\n', 'paths': [{'path': None, 'subdirGlobFilter': None, 'subdirRegexFilter': None, 'excludeGlobFilter': None, 'excludeRegexFilter': None}], 'pathGlobFilter': '*.parquet', 'isRecursiveEnabled': True, 'metadataColumnEnabled': True, 'enableFilterPattern': True, 'readMode': 'DefaultReadMode', 'genAIMetadataTableDescription': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'ParquetInputStep', 'class_pretty_name': 'Parquet', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 1462, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    sql_datosbasicos = sql(
        name="SQL_DatosBasicos",
        query="""

SELECT
    LPAD(TRIM(codigoIdentificacionInternoCliente),16,'0') AS codigoIdentificacionInternoCliente
    ,LPAD(TRIM(numeroIdentificacionCliente),14,'0') AS numeroIdentificacionCliente
FROM {{{PX_SDX}}}{{{TN_CLIENTE_DATOS_BASICOS_SHIST}}}
WHERE
    periodo = LAST_DAY('{{{P_FECHA_CORTE}}}')
    AND numeroIdentificacionCliente not rlike '(?i)eli|el|li|e'
    AND TRIM(numeroIdentificacionCliente) is not null
    AND UPPER(TRIM(segmentoCliente)) = 'PERSONAS'
    AND UPPER(TRIM(estadoCliente)) = 'S'
    AND UPPER(TRIM(marcaClienteFallecido)) = 'NO FALLECIDO'
    AND autorizacionTratamientoDatosPersonalesCliente = true
    AND LPAD(TRIM(numeroIdentificacionCliente),14,'0') NOT IN (
        SELECT
            LPAD(TRIM(numeroIdentificacionCliente),14,'0') AS numeroIdentificacionCliente
        FROM {{{PX_SDX}}}{{{TN_CLIENTE_DATOS_BASICOS_SHIST}}}
        WHERE
            periodo = LAST_DAY('{{{P_FECHA_CORTE}}}')
            AND numeroIdentificacionCliente not rlike '(?i)eli|el|li|e'
            AND TRIM(numeroIdentificacionCliente) is not null
            AND UPPER(TRIM(segmentoCliente)) = 'PERSONAS'
            AND UPPER(TRIM(estadoCliente)) = 'S'
            AND UPPER(TRIM(marcaClienteFallecido)) = 'NO FALLECIDO'
            AND autorizacionTratamientoDatosPersonalesCliente = true
        GROUP BY LPAD(TRIM(numeroIdentificacionCliente),14,'0')
        HAVING COUNT(*) > 1
    )

""",
        force_native_query=False,
        cache_table=False,
        description='',
        priority=110,
        config_override={'debugOptions': '{"executeStepAutoDebug":true}', 'query': "\nSELECT\n    LPAD(TRIM(codigoIdentificacionInternoCliente),16,'0') AS codigoIdentificacionInternoCliente\n    ,LPAD(TRIM(numeroIdentificacionCliente),14,'0') AS numeroIdentificacionCliente\nFROM {{{PX_SDX}}}{{{TN_CLIENTE_DATOS_BASICOS_SHIST}}}\nWHERE\n    periodo = LAST_DAY('{{{P_FECHA_CORTE}}}')\n    AND numeroIdentificacionCliente not rlike '(?i)\x08eli\x08|\x08el\x08|li|e'\n    AND TRIM(numeroIdentificacionCliente) is not null\n    AND UPPER(TRIM(segmentoCliente)) = 'PERSONAS'\n    AND UPPER(TRIM(estadoCliente)) = 'S'\n    AND UPPER(TRIM(marcaClienteFallecido)) = 'NO FALLECIDO'\n    AND autorizacionTratamientoDatosPersonalesCliente = true\n    AND LPAD(TRIM(numeroIdentificacionCliente),14,'0') NOT IN (\n        SELECT\n            LPAD(TRIM(numeroIdentificacionCliente),14,'0') AS numeroIdentificacionCliente\n        FROM {{{PX_SDX}}}{{{TN_CLIENTE_DATOS_BASICOS_SHIST}}}\n        WHERE\n            periodo = LAST_DAY('{{{P_FECHA_CORTE}}}')\n            AND numeroIdentificacionCliente not rlike '(?i)\x08eli\x08|\x08el\x08|li|e'\n            AND TRIM(numeroIdentificacionCliente) is not null\n            AND UPPER(TRIM(segmentoCliente)) = 'PERSONAS'\n            AND UPPER(TRIM(estadoCliente)) = 'S'\n            AND UPPER(TRIM(marcaClienteFallecido)) = 'NO FALLECIDO'\n            AND autorizacionTratamientoDatosPersonalesCliente = true\n        GROUP BY LPAD(TRIM(numeroIdentificacionCliente),14,'0')\n        HAVING COUNT(*) > 1\n    )\n", 'forceNativeQuery': False, 'cacheTable': False, 'isSaved': True, 'asyncRefresh': False, 'genAIMetadataTableDescription': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'SQLInputStep', 'class_pretty_name': 'SQL', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 1632, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )

    # Transformation nodes
    f_localizacionnoecuador = filter(
        name="F_LocalizacionNoEcuador",
        quote_sql=False,
        filter_exp="periodo = '{{{P_FECHA_CORTE}}}'",
        inputs=pi_geolocalizacion,
        description='',
        config_override={'debugOptions': {'executeStepAutoDebug': True, 'executeStepDebug': True, 'mockType': 'NoMock'}, 'filterExp': "periodo = '{{{P_FECHA_CORTE}}}'", 'quoteSql': False, 'genAIMetadataTableDescription': '', 'inputSchemas': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'FilterTransformStep', 'class_pretty_name': 'Filter', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 1802, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    t_crucedatosparr = trigger(
        name="T_CruceDatosParr",
        sql="""

SELECT
    A.dpaParroquia
    ,B.conteoPoblacion
    ,C.conteoMigrantes
    ,C.conteoMigrantes/B.conteoPoblacion*100 AS tasaMigracion
FROM Load_Catalogo_Cantones AS A
LEFT JOIN Load_Poblacion AS B
ON A.dpaParroquia = B.dpaParroquia
LEFT JOIN Load_Emigracion AS C
ON A.dpaParroquia = C.dpaParroquia

""",
        quote_sql=False,
        discard_conditions="",
        replace_with_input_dataframe=False,
        inputs=[load_catalogo_cantones, load_poblacion, load_emigracion],
        description='',
        priority=60,
        config_override={'debugOptions': {'executeStepAutoDebug': True, 'executeStepDebug': True, 'mockType': 'NoMock'}, 'sql': '\nSELECT\n    A.dpaParroquia\n    ,B.conteoPoblacion\n    ,C.conteoMigrantes\n    ,C.conteoMigrantes/B.conteoPoblacion*100 AS tasaMigracion\nFROM Load_Catalogo_Cantones AS A\nLEFT JOIN Load_Poblacion AS B\nON A.dpaParroquia = B.dpaParroquia\nLEFT JOIN Load_Emigracion AS C\nON A.dpaParroquia = C.dpaParroquia\n', 'quoteSql': False, 'discardConditions': '', 'replaceWithInputDataframe': False, 'genAIMetadataTablesDescription': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'TriggerTransformStep', 'class_pretty_name': 'Trigger', 'supported_engines': ['Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 1972, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    f_priorizacionparroquias = filter(
        name="F_PriorizacionParroquias",
        quote_sql=False,
        filter_exp="""

tasaMigracion > (
        SELECT
            percentile_approx(tasaMigracion,0.70)
        FROM T_CruceDatosParr
    )

""",
        inputs=t_crucedatosparr,
        description='',
        config_override={'debugOptions': {'executeStepAutoDebug': True, 'executeStepDebug': True, 'mockType': 'NoMock'}, 'filterExp': '\ntasaMigracion > (\n        SELECT\n            percentile_approx(tasaMigracion,0.70)\n        FROM T_CruceDatosParr\n    )\n', 'quoteSql': False, 'genAIMetadataTableDescription': '', 'inputSchemas': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'FilterTransformStep', 'class_pretty_name': 'Filter', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 2142, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    f_registrounico = filter(
        name="F_RegistroUnico",
        quote_sql=False,
        filter_exp="conteoDatos=1",
        inputs=load_localizacion,
        description='',
        priority=110,
        config_override={'debugOptions': {'executeStepAutoDebug': True, 'executeStepDebug': True, 'mockType': 'NoMock'}, 'filterExp': 'conteoDatos=1', 'quoteSql': False, 'genAIMetadataTableDescription': '', 'inputSchemas': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'FilterTransformStep', 'class_pretty_name': 'Filter', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 2312, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    f_trx = filter(
        name="F_Trx",
        quote_sql=False,
        filter_exp="""

periodo = '{{{P_FECHA_CORTE}}}'
AND (share_dig = 1
    OR
    share_dig IS NULL)

""",
        inputs=load_sharedigital,
        description='',
        config_override={'debugOptions': {'executeStepAutoDebug': True, 'executeStepDebug': True, 'mockType': 'NoMock'}, 'filterExp': "\nperiodo = '{{{P_FECHA_CORTE}}}'\nAND (share_dig = 1\n    OR\n    share_dig IS NULL)\n", 'quoteSql': False, 'genAIMetadataTableDescription': '', 'inputSchemas': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'FilterTransformStep', 'class_pretty_name': 'Filter', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 2482, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    t_clientesnoecuador = trigger(
        name="T_ClientesNoEcuador",
        sql="""

SELECT
    A.codigoIdentificacionInternoCliente
FROM SQL_DatosBasicos AS A
LEFT ANTI JOIN F_LocalizacionNoEcuador AS B
ON A.codigoIdentificacionInternoCliente = B.cifOrdenante

""",
        quote_sql=False,
        discard_conditions="",
        replace_with_input_dataframe=False,
        inputs=[sql_datosbasicos, f_localizacionnoecuador],
        description='',
        config_override={'debugOptions': {'executeStepAutoDebug': True, 'executeStepDebug': True, 'mockType': 'NoMock'}, 'sql': '\nSELECT\n    A.codigoIdentificacionInternoCliente\nFROM SQL_DatosBasicos AS A\nLEFT ANTI JOIN F_LocalizacionNoEcuador AS B\nON A.codigoIdentificacionInternoCliente = B.cifOrdenante\n', 'quoteSql': False, 'discardConditions': '', 'replaceWithInputDataframe': False, 'genAIMetadataTablesDescription': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'TriggerTransformStep', 'class_pretty_name': 'Trigger', 'supported_engines': ['Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 2652, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    t_crucepotencialparroquia = trigger(
        name="T_CrucePotencialParroquia",
        sql="""

SELECT
    A.codigoIdentificacionInternoCliente
FROM F_RegistroUnico AS A
INNER JOIN F_PriorizacionParroquias  AS B
ON A.codigoParroquiaDomicilioCliente = B.dpaParroquia

""",
        quote_sql=False,
        discard_conditions="",
        replace_with_input_dataframe=False,
        inputs=[f_registrounico, f_priorizacionparroquias],
        description='',
        config_override={'debugOptions': {'executeStepAutoDebug': True, 'executeStepDebug': True, 'mockType': 'NoMock'}, 'sql': '\nSELECT\n    A.codigoIdentificacionInternoCliente\nFROM F_RegistroUnico AS A\nINNER JOIN F_PriorizacionParroquias  AS B\nON A.codigoParroquiaDomicilioCliente = B.dpaParroquia\n', 'quoteSql': False, 'discardConditions': '', 'replaceWithInputDataframe': False, 'genAIMetadataTablesDescription': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'TriggerTransformStep', 'class_pretty_name': 'Trigger', 'supported_engines': ['Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 2822, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    t_filtrotrxnofisicas = trigger(
        name="T_FiltroTrxNoFisicas",
        sql="""

SELECT
    A.codigoIdentificacionInternoCliente
FROM T_ClientesNoEcuador AS A
INNER JOIN F_Trx AS B
ON A.codigoIdentificacionInternoCliente = B.codigoIdentificacionInternoCliente

""",
        quote_sql=False,
        discard_conditions="",
        replace_with_input_dataframe=False,
        inputs=[t_clientesnoecuador, f_trx],
        description='',
        config_override={'debugOptions': {'executeStepAutoDebug': True, 'executeStepDebug': True, 'mockType': 'NoMock'}, 'sql': '\nSELECT\n    A.codigoIdentificacionInternoCliente\nFROM T_ClientesNoEcuador AS A\nINNER JOIN F_Trx AS B\nON A.codigoIdentificacionInternoCliente = B.codigoIdentificacionInternoCliente\n', 'quoteSql': False, 'discardConditions': '', 'replaceWithInputDataframe': False, 'genAIMetadataTablesDescription': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'TriggerTransformStep', 'class_pretty_name': 'Trigger', 'supported_engines': ['Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 2992, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    t_origeninmigracion = trigger(
        name="T_OrigenInmigracion",
        sql="""

SELECT
    A.codigoIdentificacionInternoCliente
FROM T_FiltroTrxNoFisicas AS A
INNER JOIN T_CrucePotencialParroquia AS B
ON A.codigoIdentificacionInternoCliente = B.codigoIdentificacionInternoCliente

""",
        quote_sql=False,
        discard_conditions="",
        replace_with_input_dataframe=False,
        inputs=[t_crucepotencialparroquia, t_filtrotrxnofisicas],
        description='',
        config_override={'debugOptions': {'executeStepAutoDebug': True, 'executeStepDebug': True, 'mockType': 'NoMock'}, 'sql': '\nSELECT\n    A.codigoIdentificacionInternoCliente\nFROM T_FiltroTrxNoFisicas AS A\nINNER JOIN T_CrucePotencialParroquia AS B\nON A.codigoIdentificacionInternoCliente = B.codigoIdentificacionInternoCliente\n', 'quoteSql': False, 'discardConditions': '', 'replaceWithInputDataframe': False, 'genAIMetadataTablesDescription': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'TriggerTransformStep', 'class_pretty_name': 'Trigger', 'supported_engines': ['Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 3162, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )
    transformacion = trigger(
        name="Transformacion",
        sql="""

SELECT
    codigoIdentificacionInternoCliente
    ,CAST('{{{P_FECHA_CORTE}}}' AS DATE) AS periodo
    ,DATE_FORMAT('{{{P_FECHA_CORTE}}}', 'yyyyMM') AS codigoPeriodo
    ,from_utc_timestamp(current_timestamp(), 'America/Bogota') AS fechaIngesta
FROM T_OrigenInmigracion

""",
        quote_sql=False,
        discard_conditions="",
        replace_with_input_dataframe=False,
        inputs=t_origeninmigracion,
        description='',
        config_override={'debugOptions': {'executeStepAutoDebug': True, 'executeStepDebug': True, 'mockType': 'NoMock'}, 'sql': "\nSELECT\n    codigoIdentificacionInternoCliente\n    ,CAST('{{{P_FECHA_CORTE}}}' AS DATE) AS periodo\n    ,DATE_FORMAT('{{{P_FECHA_CORTE}}}', 'yyyyMM') AS codigoPeriodo\n    ,from_utc_timestamp(current_timestamp(), 'America/Bogota') AS fechaIngesta\nFROM T_OrigenInmigracion\n", 'quoteSql': False, 'discardConditions': '', 'replaceWithInputDataframe': False, 'genAIMetadataTablesDescription': '', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'TriggerTransformStep', 'class_pretty_name': 'Trigger', 'supported_engines': ['Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [{'saveMode': 'Overwrite', 'outputStepName': 'Po_Guardado', 'tableName': '{{{P_NOMBRE_TABLA}}}', 'discardTableName': '', 'extraOptions': {'checkIfEmpty': True, 'partitionBy': 'periodo', 'partitionOverwriteEnabled': True, 'partitionColumns': '', 'saveMode': 'Overwrite', 'partitions': ''}}], 'ui_configuration': {'position': {'x': 3332, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )

    # Output nodes
    po_guardado = parquet_output(
        name="Po_Guardado",
        path="s3a://s3-lagodatos-noprod-04/data/tmp/analitica/recepcionremesas/20014/",
        save_options="",
        save_mode="Overwrite",
        inputs=transformacion,
        description='',
        config_override={'debugOptions': {'executeStepAutoDebug': True, 'executeStepDebug': True, 'mockType': 'NoMock'}, 'path': 's3a://s3-lagodatos-noprod-04/data/tmp/analitica/recepcionremesas/20014/', 'saveMode': 'Overwrite', 'saveOptions': ''},
        node_overrides={'class_name': 'ParquetOutputStep', 'class_pretty_name': 'Parquet', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 3502, 'y': 289}}, 'lineage_properties': [], 'last_modified': '2026-02-12T21:21:30Z'}
    )

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "convertido_rebuilt.json")
