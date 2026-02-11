"""
DSL Compiler para Stratio Rocket

Compila un objeto Pipeline (IR) a formato JSON compatible con Stratio Rocket.

El compiler:
1. Toma el IR (Pipeline)
2. Aplica plantillas corporativas estándar
3. Genera el JSON completo que Rocket espera
"""

import json
import uuid
from datetime import datetime
from typing import Dict, Any
from py2rocket.core.pipeline import Pipeline


class RocketCompiler:
    """
    Compilador de pipelines DSL a formato JSON de Stratio Rocket.

    Transforma la representación intermedia (IR) del pipeline en el formato
    JSON completo que Stratio Rocket necesita para ejecutar el workflow.

    Incluye:
    - Settings globales (Spark, Kubernetes, Docker)
    - Configuraciones de debug
    - Health checks
    - Parámetros de sistema
    """

    # Plantilla estándar de settings corporativos
    STANDARD_SETTINGS = {
        "global": {
            "executionMode": "kubernetes",
            "dockerSettings": {
                "driverDockerImage": "{{{SparkConfigurations.SPARK_DRIVER_DOCKER_IMAGE}}}",
                "driverDockerVolumes": "{{{SparkConfigurations.SPARK_DRIVER_DOCKER_VOLUMES}}}",
                "executorDockerImage": "{{{SparkConfigurations.SPARK_EXECUTOR_DOCKER_IMAGE}}}",
                "executorDockerVolumes": "{{{SparkConfigurations.SPARK_EXECUTOR_DOCKER_VOLUMES}}}",
            },
            "userPluginsJars": [],
            "parametersLists": ["Environment", "SparkResources", "SparkConfigurations"],
            "parametersUsed": [],
            "sqlSettings": {
                "preExecutionSqlSentences": [],
                "postExecutionSqlSentences": [],
                "udfsToRegister": [],
                "udafsToRegister": [],
            },
            "kubernetesDeploymentSettings": {
                "gracePeriodSeconds": "{{{SparkConfigurations.HEALTH_CHECK_GRACE_PERIOD_SECONDS}}}",
                "intervalSeconds": "{{{SparkConfigurations.HEALTH_CHECK_INTERVAL_SECONDS}}}",
                "timeoutSeconds": "{{{SparkConfigurations.HEALTH_CHECK_FAILURES_TIMEOUT}}}",
                "maxConsecutiveFailures": "{{{SparkConfigurations.HEALTH_CHECK_MAX_CONSECUTIVE_FAILURES}}}",
                "imagePullPolicy": "IfNotPresent",
                "userEnvVariables": [],
                "userLabels": [],
                "includePostgresHealthCheck": True,
                "includeHdfsHealthCheck": True,
                "includeSparkHealthCheck": True,
                "driverPlacements": {
                    "addedPlacements": [],
                    "configurableProjectPlacementsEnabled": True,
                },
                "executorPlacements": {
                    "addedPlacements": [],
                    "configurableProjectPlacementsEnabled": True,
                },
                "driverVolumes": {"addedVolumes": {}, "excludedVolumes": []},
                "executorVolumes": {"addedVolumes": {}, "excludedVolumes": []},
            },
            "enableQualityRules": True,
            "debugSettings": {
                "forceDebugExecutionForAllSteps": False,
                "limitRecordsDebug": "{{{SparkConfigurations.DEBUG_MOCK_DATA_LIMIT}}}",
                "limitProcessingRecordsDebug": "{{{SparkConfigurations.DEBUG_PROCESSING_DATA_LIMIT}}}",
                "doNotUseCacheData": True,
                "unlimitedRecordsInProcessing": False,
                "autoInferMaxFiles": "{{{SparkConfigurations.DEBUG_AUTO_INFER_MAX_FILES_LIMIT}}}",
                "forceRunAsExecution": False,
                "forceRunAsExecutionWithMaxSteps": "{{{SparkConfigurations.DEBUG_FORCE_RUN_AS_EXECUTION_WITH_MAX_STEPS}}}",
                "executeWithSameExecutionMode": False,
                "numberOfColumnExamples": "{{{SparkConfigurations.DEBUG_NUMBER_OF_COLUMN_EXAMPLES}}}",
                "maxSizeColumnExamples": "{{{SparkConfigurations.DEBUG_MAX_SIZE_COLUMN_EXAMPLES}}}",
                "executeDataAnalysisInAllSteps": True,
            },
            "autoDebugSettings": {
                "enableAutoDebug": True,
                "forceAutoDebugExecutionForAllSteps": False,
                "doNotUseCacheData": True,
            },
            "parametersSettings": {"userDefinedParameters": []},
            "getTotalRowsByStep": False,
            "enableProjectEnvVar": True,
            "executionMetricsSettings": {"customMetricLabels": []},
        },
        "streamingSettings": {
            "window": "{{{SparkConfigurations.SPARK_STREAMING_WINDOW}}}",
            "backpressure": False,
            "blockInterval": "{{{SparkConfigurations.SPARK_STREAMING_BLOCK_INTERVAL}}}",
            "stopGracefully": True,
            "checkpointSettings": {
                "checkpointPath": "{{{SparkConfigurations.SPARK_STREAMING_CHECKPOINT_PATH}}}",
                "enableCheckpointing": True,
                "autoDeleteCheckpoint": True,
                "addTimeToCheckpointPath": False,
                "keepSameCheckpoint": False,
            },
        },
        "sparkSettings": {
            "sparkKerberos": True,
            "sparkDataStoreTls": True,
            "sparkVaultSecretList": False,
            "sparkVaultSecretListNames": [],
            "sparkConf": {
                "sparkResourcesConf": {
                    "executorMemory": "{{{SparkResources.SPARK_EXECUTOR_MEMORY}}}",
                    "executorCores": "{{{SparkResources.SPARK_EXECUTOR_CORES}}}",
                    "driverCores": "{{{SparkResources.SPARK_DRIVER_CORES}}}",
                    "driverMemory": "{{{SparkResources.SPARK_DRIVER_MEMORY}}}",
                    "limitModeDriverCores": "SOFT",
                    "limitModeDriverMemory": "GUARANTEED",
                    "limitModeExecutorCores": "SOFT",
                    "executorInstances": "{{{SparkResources.SPARK_EXECUTOR_INSTANCES}}}",
                    "enableDriverGpus": False,
                    "driverGpus": "{{{SparkResources.SPARK_DRIVER_GPUS}}}",
                    "enableExecutorGpus": False,
                    "executorGpus": "{{{SparkResources.SPARK_EXECUTOR_GPUS}}}",
                },
                "sparkHistoryServerConf": {
                    "enableHistoryServerMonitoring": False,
                    "sparkHistoryServerEventLogRotateEnable": False,
                    "sparkHistoryServerEventLogRotateMaxFileSize": "{{{SparkConfigurations.SPARK_HISTORY_SERVER_EVENT_LOG_ROTATE_MAX_FILE_SIZE}}}",
                },
                "userSparkConf": [],
                "sparkUser": "root",
                "logStagesProgress": False,
                "hdfsTokenCache": True,
                "executorExtraJavaOptions": "{{{SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS}}}",
                "stopGracefullyTimeout": "{{{SparkResources.SPARK_KUBERNETES_SHUTDOWN}}}",
                "sparkSchedulingConf": {
                    "minRegisteredResourcesRatio": "{{{SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO}}}",
                    "maxRegisteredResourcesWaitingTime": "{{{SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME}}}",
                },
                "sparkMetricsConf": {
                    "sparkMetricsEnabled": False,
                    "sparkDriverSourcesWhitelist": "{{{SparkConfigurations.SPARK_DRIVER_METRICS_SOURCES_WHITELIST}}}",
                    "sparkDriverUnregisteredMetrics": [],
                    "sparkExecutorSourcesWhitelist": "{{{SparkConfigurations.SPARK_EXECUTOR_METRICS_SOURCES_WHITELIST}}}",
                    "sparkExecutorUnregisteredMetrics": [],
                },
                "enableProjectSparkConf": True,
            },
        },
        "errorsManagement": {"genericErrorManagement": {"whenError": "Error"}},
        "pythonEnvDefinition": {
            "vEnvManagementMode": "DefaultExecutionVirtualEnv",
            "condaYamlDefinition": "name: rocket-default\n\nchannels:\n  - conda-forge\n  - nodefaults\n\ndependencies:\n  - python=3.9.*\n  - pip=25.1.*\n  - pip:\n      - mlflow==2.18.*\n      - pyarrow==14.*\n      - scikit-learn==1.*\n      - numpy==1.23.*\n      - scipy==1.*\n      - pandas==1.*\n      - petastorm==0.12.*\n      - langchain==0.3.*\n      - tiktoken==0.9.0",
            "freezeAfterDebug": False,
            "condaPackExtension": [],
            "executeCondaUnpackAfterActivate": False,
            "pySparkNativeExtensions": [],
        },
        "structuredStreamingSettings": {},
    }

    def __init__(self, pipeline: Pipeline):
        """
        Inicializa el compilador con un pipeline.

        Args:
            pipeline: Objeto Pipeline a compilar
        """
        self.pipeline = pipeline

    def _extract_parameters_used(self) -> list:
        """
        Extrae todos los parámetros referenciados en el pipeline.
        Busca patrones {{PARAMETRO}} en las queries y configuraciones.
        """
        params_used = set()

        # Parámetros de usuario definidos
        for param_name in self.pipeline.parameters.keys():
            params_used.add(param_name)

        # Parámetros estándar del sistema
        system_params = [
            "SparkConfigurations.DEBUG_AUTO_INFER_MAX_FILES_LIMIT",
            "SparkConfigurations.DEBUG_FORCE_RUN_AS_EXECUTION_WITH_MAX_STEPS",
            "SparkConfigurations.DEBUG_MAX_SIZE_COLUMN_EXAMPLES",
            "SparkConfigurations.DEBUG_MOCK_DATA_LIMIT",
            "SparkConfigurations.DEBUG_NUMBER_OF_COLUMN_EXAMPLES",
            "SparkConfigurations.DEBUG_PROCESSING_DATA_LIMIT",
            "SparkConfigurations.HEALTH_CHECK_FAILURES_TIMEOUT",
            "SparkConfigurations.HEALTH_CHECK_GRACE_PERIOD_SECONDS",
            "SparkConfigurations.HEALTH_CHECK_INTERVAL_SECONDS",
            "SparkConfigurations.HEALTH_CHECK_MAX_CONSECUTIVE_FAILURES",
            "SparkConfigurations.SPARK_DRIVER_DOCKER_IMAGE",
            "SparkConfigurations.SPARK_DRIVER_DOCKER_VOLUMES",
            "SparkConfigurations.SPARK_EXECUTOR_DOCKER_IMAGE",
            "SparkConfigurations.SPARK_EXECUTOR_DOCKER_VOLUMES",
            "SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS",
            "SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME",
            "SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO",
            "SparkResources.SPARK_DRIVER_CORES",
            "SparkResources.SPARK_DRIVER_MEMORY",
            "SparkResources.SPARK_EXECUTOR_CORES",
            "SparkResources.SPARK_EXECUTOR_INSTANCES",
            "SparkResources.SPARK_EXECUTOR_MEMORY",
            "SparkResources.SPARK_KUBERNETES_SHUTDOWN",
        ]

        params_used.update(system_params)

        return sorted(list(params_used))

    def _build_user_parameters(self) -> list:
        """Construye la lista de parámetros de usuario definidos"""
        return [
            {"customParameterName": key, "customParameterValue": value}
            for key, value in self.pipeline.parameters.items()
        ]

    def _add_ui_positions(self, nodes: list) -> list:
        """Añade posiciones UI a los nodos para visualización en Rocket"""
        x_start = 612
        y_base = 289
        x_spacing = 170

        for i, node in enumerate(nodes):
            node["uiConfiguration"] = {
                "position": {"x": x_start + (i * x_spacing), "y": y_base}
            }
            node["lastModified"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        return nodes

    def compile(self) -> Dict[str, Any]:
        """
        Compila el pipeline a formato JSON de Rocket.

        Returns:
            Diccionario con la estructura completa del pipeline en formato Rocket
        """
        # Obtener la estructura base del pipeline
        pipeline_dict = self.pipeline.to_dict()

        # Añadir posiciones UI a los nodos
        pipeline_dict["pipelineGraph"]["nodes"] = self._add_ui_positions(
            pipeline_dict["pipelineGraph"]["nodes"]
        )

        # Construir el JSON completo
        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        rocket_json = {
            "id": self.pipeline.workflow_id or str(uuid.uuid4()),
            "name": self.pipeline.name,
            "description": self.pipeline.description,
            "settings": self.STANDARD_SETTINGS.copy(),
            "pipelineGraph": pipeline_dict["pipelineGraph"],
            "executionEngine": self.pipeline.execution_engine.value,
            "workflowType": "SpartaWorkflow",
            "uiSettings": {
                "position": {"x": -2083.536303142103, "y": -859.7024044750958, "k": 4.0}
            },
            "creationDate": now,
            "lastUpdateDate": now,
            "version": 0,
            "readOnly": False,
            "releaseInProgress": False,
            "tags": [],
            "debugMode": False,
            "debugAsExecutionMaybe": False,
            "versionSparta": "3.6.3",
            "normalizedName": self.pipeline.name.lower(),
            "isHybridStreaming": False,
        }

        # Añadir listas de parámetros adicionales
        if getattr(self.pipeline, "parameters_lists", None):
            base_lists = rocket_json["settings"]["global"]["parametersLists"]
            extra_lists = [p for p in self.pipeline.parameters_lists if p]
            rocket_json["settings"]["global"]["parametersLists"] = list(
                dict.fromkeys(base_lists + extra_lists)
            )

        # Agregar sentencias SQL de pre-ejecución
        if getattr(self.pipeline, "pre_execution_sql_sentences", None):
            sql_sentences = [
                {"sentence": sentence}
                for sentence in self.pipeline.pre_execution_sql_sentences
                if sentence
            ]
            rocket_json["settings"]["global"]["sqlSettings"][
                "preExecutionSqlSentences"
            ] = sql_sentences

        # Agregar UDFs a registrar
        if getattr(self.pipeline, "udfs_to_register", None):
            udfs = [{"name": udf} for udf in self.pipeline.udfs_to_register if udf]
            rocket_json["settings"]["global"]["sqlSettings"]["udfsToRegister"] = udfs

        # Agregar UDAFs a registrar
        if getattr(self.pipeline, "udafs_to_register", None):
            udafs = [{"name": udaf} for udaf in self.pipeline.udafs_to_register if udaf]
            rocket_json["settings"]["global"]["sqlSettings"]["udafsToRegister"] = udafs

        # Agregar configuraciones Spark personalizadas
        if getattr(self.pipeline, "user_spark_conf", None):
            spark_conf = [
                {"sparkConfKey": key, "sparkConfValue": value}
                for key, value in self.pipeline.user_spark_conf.items()
                if key and value
            ]
            rocket_json["settings"]["global"]["sparkSettings"][
                "userSparkConf"
            ] = spark_conf

            # Agregar plugins de usuario si están resueltos
            if getattr(self.pipeline, "user_plugins_jars", None):
                jars = [j for j in self.pipeline.user_plugins_jars if j]
                rocket_json["settings"]["global"]["userPluginsJars"] = jars

        # Incluir el workflowMasterId si existe
        if getattr(self.pipeline, "asset_id", None):
            rocket_json["workflowMasterId"] = self.pipeline.asset_id

        # Añadir parámetros usados
        rocket_json["settings"]["global"][
            "parametersUsed"
        ] = self._extract_parameters_used()
        rocket_json["settings"]["global"]["parametersSettings"] = {
            "userDefinedParameters": self._build_user_parameters()
        }

        return rocket_json

    def to_json(self, indent: int = 2) -> str:
        """
        Compila y serializa el pipeline a JSON string.

        Args:
            indent: Nivel de indentación para el JSON

        Returns:
            String JSON formateado
        """
        compiled = self.compile()
        return json.dumps(compiled, indent=indent, ensure_ascii=False)

    def save(self, filepath: str) -> None:
        """
        Compila y guarda el pipeline en un archivo JSON.

        Args:
            filepath: Ruta donde guardar el archivo JSON
        """
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(self.to_json())
