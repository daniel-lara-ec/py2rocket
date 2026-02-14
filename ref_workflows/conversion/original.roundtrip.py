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
    params={'P_NOMBRE_TABLA': 'tmp_leads_cev_v2'},
    workflow_id="67d9dbbc-3d7b-4611-ba2f-aaefdb431a10",
    project_id='196c1c2d-5dfd-4756-ba37-80aa50d0f742',
    group_id='99beb8c9-32e7-465f-9081-137cea8adee6',
    asset_id="3d3d44bf-96bd-4f65-b731-44f14fecdbb9",
    parameters_lists=['Environment', 'SparkResources', 'SparkConfigurations', 'ParamsRecepcionRemesas'],
    raw_settings={'global': {'executionMode': 'kubernetes', 'dockerSettings': {'driverDockerImage': '{{{SparkConfigurations.SPARK_DRIVER_DOCKER_IMAGE}}}', 'driverDockerVolumes': '{{{SparkConfigurations.SPARK_DRIVER_DOCKER_VOLUMES}}}', 'executorDockerImage': '{{{SparkConfigurations.SPARK_EXECUTOR_DOCKER_IMAGE}}}', 'executorDockerVolumes': '{{{SparkConfigurations.SPARK_EXECUTOR_DOCKER_VOLUMES}}}'}, 'userPluginsJars': [], 'parametersLists': ['Environment', 'SparkResources', 'SparkConfigurations', 'ParamsRecepcionRemesas'], 'parametersUsed': ['P_FECHA_CORTE', 'P_NOMBRE_TABLA', 'SparkConfigurations.DEBUG_AUTO_INFER_MAX_FILES_LIMIT', 'SparkConfigurations.DEBUG_FORCE_RUN_AS_EXECUTION_WITH_MAX_STEPS', 'SparkConfigurations.DEBUG_MAX_SIZE_COLUMN_EXAMPLES', 'SparkConfigurations.DEBUG_MOCK_DATA_LIMIT', 'SparkConfigurations.DEBUG_NUMBER_OF_COLUMN_EXAMPLES', 'SparkConfigurations.DEBUG_PROCESSING_DATA_LIMIT', 'SparkConfigurations.HEALTH_CHECK_FAILURES_TIMEOUT', 'SparkConfigurations.HEALTH_CHECK_GRACE_PERIOD_SECONDS', 'SparkConfigurations.HEALTH_CHECK_INTERVAL_SECONDS', 'SparkConfigurations.HEALTH_CHECK_MAX_CONSECUTIVE_FAILURES', 'SparkConfigurations.SPARK_DRIVER_DOCKER_IMAGE', 'SparkConfigurations.SPARK_DRIVER_DOCKER_VOLUMES', 'SparkConfigurations.SPARK_EXECUTOR_DOCKER_IMAGE', 'SparkConfigurations.SPARK_EXECUTOR_DOCKER_VOLUMES', 'SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS', 'SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME', 'SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO', 'SparkResources.SPARK_DRIVER_CORES', 'SparkResources.SPARK_DRIVER_MEMORY', 'SparkResources.SPARK_EXECUTOR_CORES', 'SparkResources.SPARK_EXECUTOR_INSTANCES', 'SparkResources.SPARK_EXECUTOR_MEMORY', 'SparkResources.SPARK_KUBERNETES_SHUTDOWN'], 'sqlSettings': {'preExecutionSqlSentences': [], 'postExecutionSqlSentences': [], 'udfsToRegister': [], 'udafsToRegister': []}, 'kubernetesDeploymentSettings': {'gracePeriodSeconds': '{{{SparkConfigurations.HEALTH_CHECK_GRACE_PERIOD_SECONDS}}}', 'intervalSeconds': '{{{SparkConfigurations.HEALTH_CHECK_INTERVAL_SECONDS}}}', 'timeoutSeconds': '{{{SparkConfigurations.HEALTH_CHECK_FAILURES_TIMEOUT}}}', 'maxConsecutiveFailures': '{{{SparkConfigurations.HEALTH_CHECK_MAX_CONSECUTIVE_FAILURES}}}', 'imagePullPolicy': 'IfNotPresent', 'userEnvVariables': [], 'userLabels': [], 'logLevel': '', 'includePostgresHealthCheck': True, 'includeHdfsHealthCheck': True, 'includeSparkHealthCheck': True, 'driverPlacements': {'addedPlacements': [], 'configurableProjectPlacementsEnabled': True}, 'executorPlacements': {'addedPlacements': [], 'configurableProjectPlacementsEnabled': True}, 'driverVolumes': {'addedVolumes': {}, 'excludedVolumes': []}, 'executorVolumes': {'addedVolumes': {}, 'excludedVolumes': []}}, 'enableQualityRules': True, 'debugSettings': {'forceDebugExecutionForAllSteps': False, 'limitRecordsDebug': '{{{SparkConfigurations.DEBUG_MOCK_DATA_LIMIT}}}', 'limitProcessingRecordsDebug': '{{{SparkConfigurations.DEBUG_PROCESSING_DATA_LIMIT}}}', 'doNotUseCacheData': True, 'unlimitedRecordsInProcessing': False, 'autoInferMaxFiles': '{{{SparkConfigurations.DEBUG_AUTO_INFER_MAX_FILES_LIMIT}}}', 'forceRunAsExecution': False, 'forceRunAsExecutionWithMaxSteps': '{{{SparkConfigurations.DEBUG_FORCE_RUN_AS_EXECUTION_WITH_MAX_STEPS}}}', 'executeWithSameExecutionMode': False, 'numberOfColumnExamples': '{{{SparkConfigurations.DEBUG_NUMBER_OF_COLUMN_EXAMPLES}}}', 'maxSizeColumnExamples': '{{{SparkConfigurations.DEBUG_MAX_SIZE_COLUMN_EXAMPLES}}}', 'executeDataAnalysisInAllSteps': True}, 'autoDebugSettings': {'enableAutoDebug': True, 'forceAutoDebugExecutionForAllSteps': False, 'doNotUseCacheData': True}, 'parametersSettings': {'userDefinedParameters': [{'customParameterName': 'P_NOMBRE_TABLA', 'customParameterValue': 'tmp_leads_cev_v2'}]}, 'getTotalRowsByStep': False, 'enableProjectEnvVar': True, 'executionMetricsSettings': {'customMetricLabels': []}}, 'streamingSettings': {'window': '2s', 'backpressure': False, 'blockInterval': '100ms', 'stopGracefully': True, 'checkpointSettings': {'checkpointPath': 'tmp/checkpoint', 'enableCheckpointing': True, 'autoDeleteCheckpoint': True, 'addTimeToCheckpointPath': False, 'keepSameCheckpoint': False}}, 'sparkSettings': {'sparkKerberos': True, 'sparkDataStoreTls': True, 'sparkVaultSecretList': False, 'sparkVaultSecretListNames': [], 'sparkConf': {'sparkResourcesConf': {'executorMemory': '{{{SparkResources.SPARK_EXECUTOR_MEMORY}}}', 'executorCores': '{{{SparkResources.SPARK_EXECUTOR_CORES}}}', 'driverCores': '{{{SparkResources.SPARK_DRIVER_CORES}}}', 'driverMemory': '{{{SparkResources.SPARK_DRIVER_MEMORY}}}', 'limitModeDriverCores': 'SOFT', 'limitModeDriverMemory': 'GUARANTEED', 'limitModeExecutorCores': 'SOFT', 'executorTaskParallelism': '', 'sparkParallelism': '', 'executorInstances': '{{{SparkResources.SPARK_EXECUTOR_INSTANCES}}}', 'enableDriverGpus': False, 'driverGpus': '1', 'enableExecutorGpus': False, 'executorGpus': '1'}, 'sparkHistoryServerConf': {'enableHistoryServerMonitoring': False, 'sparkHistoryServerEventLogRotateEnable': False, 'sparkHistoryServerEventLogRotateMaxFileSize': '128m'}, 'userSparkConf': [], 'sparkUser': 'root', 'logStagesProgress': True, 'hdfsTokenCache': True, 'executorExtraJavaOptions': '{{{SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS}}}', 'stopGracefullyTimeout': '{{{SparkResources.SPARK_KUBERNETES_SHUTDOWN}}}', 'sparkSchedulingConf': {'minRegisteredResourcesRatio': '{{{SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO}}}', 'maxRegisteredResourcesWaitingTime': '{{{SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME}}}'}, 'sparkMetricsConf': {'sparkMetricsEnabled': False, 'sparkDriverSourcesWhitelist': 'System,jvm,DAGScheduler,BlockManager', 'sparkDriverUnregisteredMetrics': [], 'sparkExecutorSourcesWhitelist': 'System,jvm,executor', 'sparkExecutorUnregisteredMetrics': []}, 'enableProjectSparkConf': True}}, 'errorsManagement': {'genericErrorManagement': {'whenError': 'Error'}}, 'pythonEnvDefinition': {'vEnvManagementMode': 'DefaultExecutionVirtualEnv', 'condaYamlDefinition': 'name: rocket-default\n\nchannels:\n  - conda-forge\n  - nodefaults\n\ndependencies:\n  - python=3.9.*\n  - pip=25.1.*\n  - pip:\n      - mlflow==2.18.*\n      - pyarrow==14.*\n      - scikit-learn==1.*\n      - numpy==1.23.*\n      - scipy==1.*\n      - pandas==1.*\n      - petastorm==0.12.*\n      - langchain==0.3.*\n      - tiktoken==0.9.0', 'freezeAfterDebug': False, 'condaPackExtension': [], 'executeCondaUnpackAfterActivate': False, 'pySparkNativeExtensions': []}, 'structuredStreamingSettings': {}},
    raw_ui_settings={'position': {'x': 110.1468294033292, 'y': -170.52837289778995, 'k': 0.852777777777778}},
    raw_metadata={'group': {'id': '99beb8c9-32e7-465f-9081-137cea8adee6', 'name': '/home/cda-paucordo-sandbox/developer/recepcion-remesas/transformaciones/remesas/execution/data/mdt-execution/layers/13-layer-clientes-exterior'}, 'groupId': '99beb8c9-32e7-465f-9081-137cea8adee6', 'projectId': '196c1c2d-5dfd-4756-ba37-80aa50d0f742', 'versionSparta': '3.6.5', 'creationDate': '2026-02-10T16:42:30Z', 'lastUpdateDate': '2026-02-12T20:52:47Z', 'version': 0, 'readOnly': False, 'releaseInProgress': False, 'tags': [], 'debugMode': False, 'debugAsExecutionMaybe': False, 'normalizedName': 'pl-transformacion-zp-mdp-capa', 'isHybridStreaming': False, 'workflowType': 'SpartaWorkflow', 'workflowMasterId': '3d3d44bf-96bd-4f65-b731-44f14fecdbb9'},
    annotations=[],
    node_groups=[],
    raw_nodes_order=['SQL_DatosBasicos', 'Pi_Geolocalizacion', 'Load_ShareDigital', 'T_ClientesNoEcuador', 'F_LocalizacionNoEcuador', 'F_Trx', 'T_FiltroTrxNoFisicas', 'Transformacion', 'Po_Guardado', 'Load_Localizacion', 'F_RegistroUnico', 'T_CruceDatosParr', 'Load_Catalogo_Cantones', 'Load_Poblacion', 'Load_Emigracion', 'F_PriorizacionParroquias', 'T_CrucePotencialParroquia', 'T_OrigenInmigracion'],
    raw_edges_order=[{'origin': 'Pi_Geolocalizacion', 'destination': 'F_LocalizacionNoEcuador', 'dataType': 'ValidData'}, {'origin': 'SQL_DatosBasicos', 'destination': 'T_ClientesNoEcuador', 'dataType': 'ValidData'}, {'origin': 'F_LocalizacionNoEcuador', 'destination': 'T_ClientesNoEcuador', 'dataType': 'ValidData'}, {'origin': 'Load_ShareDigital', 'destination': 'F_Trx', 'dataType': 'ValidData'}, {'origin': 'T_ClientesNoEcuador', 'destination': 'T_FiltroTrxNoFisicas', 'dataType': 'ValidData'}, {'origin': 'F_Trx', 'destination': 'T_FiltroTrxNoFisicas', 'dataType': 'ValidData'}, {'origin': 'Transformacion', 'destination': 'Po_Guardado', 'dataType': 'ValidData'}, {'origin': 'Load_Localizacion', 'destination': 'F_RegistroUnico', 'dataType': 'ValidData'}, {'origin': 'Load_Catalogo_Cantones', 'destination': 'T_CruceDatosParr', 'dataType': 'ValidData'}, {'origin': 'Load_Poblacion', 'destination': 'T_CruceDatosParr', 'dataType': 'ValidData'}, {'origin': 'Load_Emigracion', 'destination': 'T_CruceDatosParr', 'dataType': 'ValidData'}, {'origin': 'T_CruceDatosParr', 'destination': 'F_PriorizacionParroquias', 'dataType': 'ValidData'}, {'origin': 'F_RegistroUnico', 'destination': 'T_CrucePotencialParroquia', 'dataType': 'ValidData'}, {'origin': 'F_PriorizacionParroquias', 'destination': 'T_CrucePotencialParroquia', 'dataType': 'ValidData'}, {'origin': 'T_CrucePotencialParroquia', 'destination': 'T_OrigenInmigracion', 'dataType': 'ValidData'}, {'origin': 'T_FiltroTrxNoFisicas', 'destination': 'T_OrigenInmigracion', 'dataType': 'ValidData'}, {'origin': 'T_OrigenInmigracion', 'destination': 'Transformacion', 'dataType': 'ValidData'}],
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
FROM dsc_medios_pago.ZP_BP_Mdp_TD_InecGeoDpaParroquia
""",
        force_native_query=False,
        cache_table=False,
        description='',
        priority=10,
        config_override={'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'isSaved': True, 'query': 'SELECT DISTINCT dpaParroquia \r\nFROM dsc_medios_pago.ZP_BP_Mdp_TD_InecGeoDpaParroquia', 'forceNativeQuery': False, 'cacheTable': False, 'genAIMetadataColumns': '', 'asyncRefresh': False},
        node_overrides={'class_name': 'SQLInputStep', 'class_pretty_name': 'SQL', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 225.4659734450521, 'y': 691.7867753718463}}, 'lineage_properties': [], 'last_modified': '2026-01-07T20:36:39Z'}
    )
    load_emigracion = sql(
        name="Load_Emigracion",
        query="""
SELECT
    parroq AS dpaParroquia
    ,COUNT(1) AS conteoMigrantes
FROM dsc_medios_pago.ZP_BP_Mdp_TD_InecCenso2022Emigracion
WHERE e03 + (2025-e01) >= 18
AND e03 != 999
GROUP BY parroq
""",
        force_native_query=False,
        cache_table=False,
        description='',
        config_override={'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'isSaved': True, 'query': 'SELECT \r\n    parroq AS dpaParroquia\r\n    ,COUNT(1) AS conteoMigrantes\r\nFROM dsc_medios_pago.ZP_BP_Mdp_TD_InecCenso2022Emigracion\r\nWHERE e03 + (2025-e01) >= 18\r\nAND e03 != 999\r\nGROUP BY parroq', 'forceNativeQuery': False, 'cacheTable': False, 'genAIMetadataColumns': '', 'asyncRefresh': False},
        node_overrides={'class_name': 'SQLInputStep', 'class_pretty_name': 'SQL', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 228.94654250925464, 'y': 869.1158581836166}}, 'lineage_properties': [], 'last_modified': '2026-01-07T20:38:59Z'}
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
    prd_gob_reg_clientes.ZP_BP_Par_Cli_TN_ClienteLocalizacion
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
        config_override={'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'isSaved': True, 'query': "SELECT\n    codigoIdentificacionInternoCliente,\n    provinciaDomicilioCliente AS provincia,\n    codigoParroquiaDomicilioCliente,\n    ROW_NUMBER() OVER (\n        PARTITION BY\n            codigoIdentificacionInternoCliente\n        ORDER BY\n            codigoParroquiaDomicilioCliente\n    ) AS conteoDatos\nFROM\n    prd_gob_reg_clientes.ZP_BP_Par_Cli_TN_ClienteLocalizacion\nWHERE\n    tipoDireccion = 'Domicilio'\n    AND codigoPaisDomicilioCliente = 'EC'\n    AND UPPER(TRIM(codigoProvinciaDomicilioCliente)) IN (\n        '01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24'\n    )", 'forceNativeQuery': False, 'cacheTable': False, 'genAIMetadataColumns': '', 'asyncRefresh': False},
        node_overrides={'class_name': 'SQLInputStep', 'class_pretty_name': 'SQL', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 239.0942991919295, 'y': 592.7719957658343}}, 'lineage_properties': [], 'last_modified': '2026-01-06T21:29:01Z'}
    )
    load_poblacion = sql(
        name="Load_Poblacion",
        query="""
SELECT
    parroq AS dpaParroquia
    ,COUNT(1) AS conteoPoblacion
FROM dsc_medios_pago.ZP_BP_Mdp_TD_InecCenso2022Poblacion
WHERE p03 >= 18
GROUP BY parroq
""",
        force_native_query=False,
        cache_table=False,
        description='',
        config_override={'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'isSaved': True, 'query': 'SELECT \r\n    parroq AS dpaParroquia\r\n    ,COUNT(1) AS conteoPoblacion\r\nFROM dsc_medios_pago.ZP_BP_Mdp_TD_InecCenso2022Poblacion\r\nWHERE p03 >= 18\r\nGROUP BY parroq', 'forceNativeQuery': False, 'cacheTable': False, 'genAIMetadataColumns': '', 'asyncRefresh': False},
        node_overrides={'class_name': 'SQLInputStep', 'class_pretty_name': 'SQL', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 226.64145648166254, 'y': 780.2248095693869}}, 'lineage_properties': [], 'last_modified': '2026-01-07T20:37:52Z'}
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
        config_override={'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'isSaved': True, 'query': 'SELECT \r\n    codigoIdentificacionInternoCliente\r\n    ,share_dig\r\n    ,periodo\r\nFROM dsc_medios_pago.ZP_BP_Mdp_TM_CapaShareDigital', 'forceNativeQuery': False, 'cacheTable': False, 'genAIMetadataColumns': '', 'asyncRefresh': False},
        node_overrides={'class_name': 'SQLInputStep', 'class_pretty_name': 'SQL', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 423.45652770996094, 'y': 287.7342071533203}}, 'lineage_properties': [], 'last_modified': '2026-02-10T18:08:55Z'}
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
        config_override={'excludeGlobFilter': '', 'inputOptions': '', 'path': 's3a://s3-lagodatos-noprod-04/data/tmp/analitica/20014/layerRemesasGeolocalizacionProvEcu', 'subdirGlobFilter': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'isSaved': True, 'subdirRegexFilter': '', 'readMode': 'DefaultReadMode', 'excludeRegexFilter': '', 'genAIMetadataColumns': '', 'schema.sparkSchema': ''},
        node_overrides={'class_name': 'ParquetInputStep', 'class_pretty_name': 'Parquet', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 235.8739013671875, 'y': 483.0015563964844}}, 'lineage_properties': [], 'last_modified': '2026-02-10T16:59:21Z'}
    )
    sql_datosbasicos = sql(
        name="SQL_DatosBasicos",
        query="""
SELECT
    LPAD(TRIM(codigoIdentificacionInternoCliente),16,'0') AS codigoIdentificacionInternoCliente
    ,LPAD(TRIM(numeroIdentificacionCliente),14,'0') AS numeroIdentificacionCliente
FROM prd_gob_reg_clientes.ZP_BP_Par_Cli_TN_ClienteDatosBasicos_shist
WHERE
    periodo = LAST_DAY('{{{P_FECHA_CORTE}}}')
    AND numeroIdentificacionCliente not rlike '(?i)\beli\b|\bel\b|li|e'
    AND TRIM(numeroIdentificacionCliente) is not null
    AND UPPER(TRIM(segmentoCliente)) = 'PERSONAS'
    AND UPPER(TRIM(estadoCliente)) = 'S'
    AND UPPER(TRIM(marcaClienteFallecido)) = 'NO FALLECIDO'
    AND autorizacionTratamientoDatosPersonalesCliente = true
    AND LPAD(TRIM(numeroIdentificacionCliente),14,'0') NOT IN (
        SELECT
            LPAD(TRIM(numeroIdentificacionCliente),14,'0') AS numeroIdentificacionCliente
        FROM prd_gob_reg_clientes.ZP_BP_Par_Cli_TN_ClienteDatosBasicos_shist
        WHERE
            periodo = LAST_DAY('{{{P_FECHA_CORTE}}}')
            AND numeroIdentificacionCliente not rlike '(?i)\beli\b|\bel\b|li|e'
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
        description='Carga y transformación de los datos básicos',
        priority=110,
        config_override={'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}', 'isSaved': True, 'query': "SELECT\r\n    LPAD(TRIM(codigoIdentificacionInternoCliente),16,'0') AS codigoIdentificacionInternoCliente\r\n    ,LPAD(TRIM(numeroIdentificacionCliente),14,'0') AS numeroIdentificacionCliente\r\nFROM prd_gob_reg_clientes.ZP_BP_Par_Cli_TN_ClienteDatosBasicos_shist\r\nWHERE\r\n    periodo = LAST_DAY('{{{P_FECHA_CORTE}}}')\r\n    AND numeroIdentificacionCliente not rlike '(?i)\\beli\\b|\\bel\\b|li|e'\r\n    AND TRIM(numeroIdentificacionCliente) is not null\r\n    AND UPPER(TRIM(segmentoCliente)) = 'PERSONAS'\r\n    AND UPPER(TRIM(estadoCliente)) = 'S'\r\n    AND UPPER(TRIM(marcaClienteFallecido)) = 'NO FALLECIDO'\r\n    AND autorizacionTratamientoDatosPersonalesCliente = true\r\n    AND LPAD(TRIM(numeroIdentificacionCliente),14,'0') NOT IN (\r\n        SELECT\r\n            LPAD(TRIM(numeroIdentificacionCliente),14,'0') AS numeroIdentificacionCliente\r\n        FROM prd_gob_reg_clientes.ZP_BP_Par_Cli_TN_ClienteDatosBasicos_shist\r\n        WHERE\r\n            periodo = LAST_DAY('{{{P_FECHA_CORTE}}}')\r\n            AND numeroIdentificacionCliente not rlike '(?i)\\beli\\b|\\bel\\b|li|e'\r\n            AND TRIM(numeroIdentificacionCliente) is not null\r\n            AND UPPER(TRIM(segmentoCliente)) = 'PERSONAS'\r\n            AND UPPER(TRIM(estadoCliente)) = 'S'\r\n            AND UPPER(TRIM(marcaClienteFallecido)) = 'NO FALLECIDO'\r\n            AND autorizacionTratamientoDatosPersonalesCliente = true\r\n        GROUP BY LPAD(TRIM(numeroIdentificacionCliente),14,'0')\r\n        HAVING COUNT(*) > 1\r\n    )", 'forceNativeQuery': False, 'cacheTable': False, 'genAIMetadataColumns': '', 'asyncRefresh': False},
        node_overrides={'class_name': 'SQLInputStep', 'class_pretty_name': 'SQL', 'supported_engines': ['Batch', 'Hybrid'], 'supported_data_relations': ['ValidData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 233.8739013671875, 'y': 381.0015563964844}}, 'lineage_properties': [], 'last_modified': '2026-02-12T20:48:01Z'}
    )

    # Transformation nodes
    f_localizacionnoecuador = filter(
        name="F_LocalizacionNoEcuador",
        quote_sql=False,
        filter_exp="periodo = '{{{P_FECHA_CORTE}}}'",
        inputs=pi_geolocalizacion,
        description='',
        config_override={'quoteSql': False, 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'inputSchemas': '', 'filterExp': "periodo = '{{{P_FECHA_CORTE}}}'", 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'FilterTransformStep', 'class_pretty_name': 'Filter', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'supported_data_relations': ['ValidData', 'DiscardedData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 415.8739013671875, 'y': 483.0015563964844}}, 'lineage_properties': [], 'last_modified': '2026-02-12T20:48:06Z'}
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
        config_override={'sql': 'SELECT \n    A.dpaParroquia\n    ,B.conteoPoblacion\n    ,C.conteoMigrantes\n    ,C.conteoMigrantes/B.conteoPoblacion*100 AS tasaMigracion\nFROM Load_Catalogo_Cantones AS A\nLEFT JOIN Load_Poblacion AS B\nON A.dpaParroquia = B.dpaParroquia\nLEFT JOIN Load_Emigracion AS C\nON A.dpaParroquia = C.dpaParroquia', 'quoteSql': False, 'discardConditions': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'replaceWithInputDataframe': False, 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'TriggerTransformStep', 'class_pretty_name': 'Trigger', 'supported_engines': ['Hybrid'], 'supported_data_relations': ['ValidData', 'DiscardedData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 428.155223361136, 'y': 859.2692068116326}}, 'lineage_properties': [], 'last_modified': '2025-11-24T21:05:06Z'}
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
        config_override={'quoteSql': False, 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'inputSchemas': '', 'filterExp': 'tasaMigracion > (\r\n        SELECT \r\n            percentile_approx(tasaMigracion,0.70)\r\n        FROM T_CruceDatosParr\r\n    )', 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'FilterTransformStep', 'class_pretty_name': 'Filter', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'supported_data_relations': ['ValidData', 'DiscardedData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 428.6257930471851, 'y': 740.7047808573178}}, 'lineage_properties': [], 'last_modified': '2026-02-10T18:57:32Z'}
    )
    f_registrounico = filter(
        name="F_RegistroUnico",
        quote_sql=False,
        filter_exp="conteoDatos=1",
        inputs=load_localizacion,
        description='',
        priority=110,
        config_override={'quoteSql': False, 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'inputSchemas': '', 'filterExp': 'conteoDatos=1', 'errorTableName': ''},
        node_overrides={'class_name': 'FilterTransformStep', 'class_pretty_name': 'Filter', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'supported_data_relations': ['ValidData', 'DiscardedData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 418.9949034399764, 'y': 592.7719957658343}}, 'lineage_properties': [], 'last_modified': '2025-10-16T19:12:16Z'}
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
        config_override={'quoteSql': False, 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'inputSchemas': '', 'filterExp': "periodo = '{{{P_FECHA_CORTE}}}'\r\nAND (share_dig = 1\r\n    OR \r\n    share_dig IS NULL)", 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'FilterTransformStep', 'class_pretty_name': 'Filter', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'supported_data_relations': ['ValidData', 'DiscardedData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 600.9985813488026, 'y': 287.6321239941017}}, 'lineage_properties': [], 'last_modified': '2026-02-12T20:47:48Z'}
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
        config_override={'sql': 'SELECT  \r\n    A.codigoIdentificacionInternoCliente\r\nFROM SQL_DatosBasicos AS A\r\nLEFT ANTI JOIN F_LocalizacionNoEcuador AS B\r\nON A.codigoIdentificacionInternoCliente = B.cifOrdenante', 'quoteSql': False, 'discardConditions': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'replaceWithInputDataframe': False, 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'TriggerTransformStep', 'class_pretty_name': 'Trigger', 'supported_engines': ['Hybrid'], 'supported_data_relations': ['ValidData', 'DiscardedData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 415.6051330566406, 'y': 380.5390930175781}}, 'lineage_properties': [], 'last_modified': '2026-02-10T18:04:34Z'}
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
        config_override={'sql': 'SELECT\r\n    A.codigoIdentificacionInternoCliente\r\nFROM F_RegistroUnico AS A\r\nINNER JOIN F_PriorizacionParroquias  AS B\r\nON A.codigoParroquiaDomicilioCliente = B.dpaParroquia', 'quoteSql': False, 'discardConditions': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'replaceWithInputDataframe': False, 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'TriggerTransformStep', 'class_pretty_name': 'Trigger', 'supported_engines': ['Hybrid'], 'supported_data_relations': ['ValidData', 'DiscardedData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 596.3762950678073, 'y': 644.6940129799258}}, 'lineage_properties': [], 'last_modified': '2026-02-10T18:26:58Z'}
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
        config_override={'sql': 'SELECT  \r\n    A.codigoIdentificacionInternoCliente\r\nFROM T_ClientesNoEcuador AS A\r\nINNER JOIN F_Trx AS B\r\nON A.codigoIdentificacionInternoCliente = B.codigoIdentificacionInternoCliente', 'quoteSql': False, 'discardConditions': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'replaceWithInputDataframe': False, 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'TriggerTransformStep', 'class_pretty_name': 'Trigger', 'supported_engines': ['Hybrid'], 'supported_data_relations': ['ValidData', 'DiscardedData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 596.700068956432, 'y': 382.4395348953401}}, 'lineage_properties': [], 'last_modified': '2026-02-10T18:16:01Z'}
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
        config_override={'sql': 'SELECT  \r\n    A.codigoIdentificacionInternoCliente\r\nFROM T_FiltroTrxNoFisicas AS A\r\nINNER JOIN T_CrucePotencialParroquia AS B\r\nON A.codigoIdentificacionInternoCliente = B.codigoIdentificacionInternoCliente', 'quoteSql': False, 'discardConditions': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'replaceWithInputDataframe': False, 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'TriggerTransformStep', 'class_pretty_name': 'Trigger', 'supported_engines': ['Hybrid'], 'supported_data_relations': ['ValidData', 'DiscardedData'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 596.3762837449051, 'y': 482.14388371162545}}, 'lineage_properties': [], 'last_modified': '2026-02-10T18:27:44Z'}
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
        config_override={'sql': "SELECT\r\n    codigoIdentificacionInternoCliente\r\n    ,CAST('{{{P_FECHA_CORTE}}}' AS DATE) AS periodo\r\n    ,DATE_FORMAT('{{{P_FECHA_CORTE}}}', 'yyyyMM') AS codigoPeriodo\r\n    ,from_utc_timestamp(current_timestamp(), 'America/Bogota') AS fechaIngesta\r\nFROM T_OrigenInmigracion", 'quoteSql': False, 'discardConditions': '', 'genAIMetadataTableDescription': '', 'debugOptions': '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}', 'isSaved': True, 'replaceWithInputDataframe': False, 'genAIMetadataColumns': ''},
        node_overrides={'class_name': 'TriggerTransformStep', 'class_pretty_name': 'Trigger', 'supported_engines': ['Hybrid'], 'supported_data_relations': ['ValidData', 'DiscardedData'], 'outputs_writer': [{'saveMode': 'Overwrite', 'outputStepName': 'Po_Guardado', 'tableName': '{{{P_NOMBRE_TABLA}}}', 'discardTableName': '', 'extraOptions': {'checkIfEmpty': True, 'partitionBy': 'periodo', 'partitionOverwriteEnabled': True, 'partitionColumns': '', 'saveMode': 'Overwrite', 'partitions': ''}}], 'ui_configuration': {'position': {'x': 764.1157362809491, 'y': 484.21857575713796}}, 'lineage_properties': [], 'last_modified': '2026-02-12T20:48:32Z'}
    )

    # Output nodes
    po_guardado = parquet_output(
        name="Po_Guardado",
        path="s3a://s3-lagodatos-noprod-04/data/tmp/analitica/recepcionremesas/20014/",
        save_options="",
        inputs=transformacion,
        description='',
        config_override={'path': 's3a://s3-lagodatos-noprod-04/data/tmp/analitica/recepcionremesas/20014/', 'saveOptions': '', 'isSaved': True},
        node_overrides={'class_name': 'ParquetOutputStep', 'class_pretty_name': 'Parquet', 'supported_engines': ['Streaming', 'Batch', 'Hybrid'], 'outputs_writer': [], 'ui_configuration': {'position': {'x': 764.5186055130558, 'y': 377.030581478531}}, 'lineage_properties': [], 'last_modified': '2026-02-10T18:05:07Z', 'include_supported_data_relations': False, 'include_debug_options': False}
    )

if __name__ == "__main__":
    # Construir el pipeline
    pipe = workflow()

    # Compilar a JSON
    build(pipe, "original_rebuilt.json")
