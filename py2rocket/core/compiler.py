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
from copy import deepcopy
from datetime import datetime
from typing import Dict, Any
from py2rocket.core.pipeline import (
    Pipeline,
    SqlSentence,
    ToRegister,
    PythonEnvDefinition,
)


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
                "logLevel": "",
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
            "window": "2s",
            "backpressure": False,
            "blockInterval": "100ms",
            "stopGracefully": True,
            "checkpointSettings": {
                "checkpointPath": "tmp/checkpoint",
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
                    "executorTaskParallelism": "",
                    "sparkParallelism": "",
                    "executorInstances": "{{{SparkResources.SPARK_EXECUTOR_INSTANCES}}}",
                    "enableDriverGpus": False,
                    "driverGpus": "1",
                    "enableExecutorGpus": False,
                    "executorGpus": "1",
                },
                "sparkHistoryServerConf": {
                    "enableHistoryServerMonitoring": False,
                    "sparkHistoryServerEventLogRotateEnable": False,
                    "sparkHistoryServerEventLogRotateMaxFileSize": "128m",
                },
                "userSparkConf": [],
                "sparkUser": "root",
                "logStagesProgress": True,
                "hdfsTokenCache": True,
                "executorExtraJavaOptions": "{{{SparkConfigurations.SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS}}}",
                "stopGracefullyTimeout": "{{{SparkResources.SPARK_KUBERNETES_SHUTDOWN}}}",
                "sparkSchedulingConf": {
                    "minRegisteredResourcesRatio": "{{{SparkConfigurations.SPARK_MIN_REGISTERED_RESOURCES_RATIO}}}",
                    "maxRegisteredResourcesWaitingTime": "{{{SparkConfigurations.SPARK_MAX_REGISTERED_RESOURCES_WAITING_TIME}}}",
                },
                "sparkMetricsConf": {
                    "sparkMetricsEnabled": False,
                    "sparkDriverSourcesWhitelist": "System,jvm,DAGScheduler,BlockManager",
                    "sparkDriverUnregisteredMetrics": [],
                    "sparkExecutorSourcesWhitelist": "System,jvm,executor",
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

    @staticmethod
    def _serialize_sql_sentence_items(items: list) -> list:
        """Serializa sentencias SQL (str | SqlSentence | dict) a lista de dicts."""
        serialized = []
        for item in items or []:
            if not item:
                continue
            if isinstance(item, SqlSentence):
                serialized.append(item.to_dict())
            elif isinstance(item, str):
                serialized.append({"sentence": item})
            elif isinstance(item, dict):
                sentence = item.get("sentence")
                if sentence:
                    serialized.append({"sentence": sentence})
        return serialized

    @staticmethod
    def _serialize_to_register_items(items: list) -> list:
        """Serializa registros (str | ToRegister | dict) a lista de dicts."""
        serialized = []
        for item in items or []:
            if not item:
                continue
            if isinstance(item, ToRegister):
                serialized.append(item.to_dict())
            elif isinstance(item, str):
                serialized.append({"name": item})
            elif isinstance(item, dict):
                name = item.get("name")
                if name:
                    serialized.append({"name": name})
        return serialized

    @staticmethod
    def _serialize_python_env_definition(value: Any) -> Dict[str, Any]:
        """Serializa python_env_definition (objeto o dict) a formato Rocket."""
        if isinstance(value, PythonEnvDefinition):
            return value.to_dict()
        if isinstance(value, dict):
            return value
        return {}

    def _extract_parameters_used(self) -> list:
        """
        Extrae todos los parámetros referenciados en el pipeline.
        Busca patrones {{{PARAMETRO}}} en las queries y configuraciones.
        """
        import re

        params_used = set()

        # Parámetros de usuario definidos
        for param_name in self.pipeline.parameters.keys():
            params_used.add(param_name)

        # Extraer parámetros usados en queries y configuraciones de nodos
        # Patrón para detectar {{{PARAM}}} o {{{List.PARAM}}}
        param_pattern = re.compile(r"\{\{\{([^}]+)\}\}\}")

        for node in self.pipeline.nodes:
            # Buscar en el configuration completo (como string JSON)
            config_str = str(node.configuration)
            matches = param_pattern.findall(config_str)
            for match in matches:
                # Si el parámetro no tiene prefijo de lista (SparkConfigurations, SparkResources, etc.)
                # considerarlo como parámetro de usuario
                if not any(
                    match.startswith(prefix)
                    for prefix in [
                        "SparkConfigurations.",
                        "SparkResources.",
                        "Environment.",
                    ]
                ):
                    params_used.add(match)

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
            if "uiConfiguration" not in node:
                node["uiConfiguration"] = {
                    "position": {"x": x_start + (i * x_spacing), "y": y_base}
                }
            if "lastModified" not in node:
                node["lastModified"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        return nodes

    @staticmethod
    def _format_pyspark_code_fields(nodes: list) -> None:
        """Formatea pythonCode de nodos PySpark usando black cuando está disponible."""
        try:
            import black
        except ImportError:
            return

        pyspark_classes = {"PySparkTransformerStep", "PySparkTransformStep"}

        for node in nodes or []:
            if not isinstance(node, dict):
                continue
            if node.get("className") not in pyspark_classes:
                continue

            configuration = node.get("configuration")
            if not isinstance(configuration, dict):
                continue

            python_code = configuration.get("pythonCode")
            if not isinstance(python_code, str) or not python_code.strip():
                continue

            try:
                formatted = black.format_str(python_code, mode=black.Mode())
            except (black.InvalidInput, SyntaxError, ValueError):
                continue

            configuration["pythonCode"] = formatted.rstrip("\n")

    @staticmethod
    def _trim_surrounding_blank_lines(text: str) -> str:
        """Elimina líneas vacías al inicio y al final de un bloque de texto."""
        import re

        # Eliminar solo líneas en blanco al inicio y final, preservando el contenido
        # interno tal cual (incluyendo escapes, regex y estilo de salto de línea).
        trimmed = re.sub(r"^(?:[ \t]*\r?\n)+", "", text)
        trimmed = re.sub(r"(?:\r?\n[ \t]*)+$", "", trimmed)
        return trimmed

    @staticmethod
    def _trim_sql_boundary_blank_lines(nodes: list) -> None:
        """Recorta líneas vacías externas en SQL Input y Trigger."""
        target_fields = {
            "SQLInputStep": "query",
            "TriggerTransformStep": "sql",
        }

        for node in nodes or []:
            if not isinstance(node, dict):
                continue

            class_name = node.get("className")
            field_name = target_fields.get(class_name)
            if not field_name:
                continue

            configuration = node.get("configuration")
            if not isinstance(configuration, dict):
                continue

            field_value = configuration.get(field_name)
            if not isinstance(field_value, str):
                continue

            configuration[field_name] = RocketCompiler._trim_surrounding_blank_lines(
                field_value
            )

    def compile(self, format_pyspark_code: bool = False) -> Dict[str, Any]:
        """
        Compila el pipeline a formato JSON de Rocket.

        Args:
            format_pyspark_code: Si es True, formatea campos pythonCode de nodos
                PySpark con black antes de generar el JSON final.

        Returns:
            Diccionario con la estructura completa del pipeline en formato Rocket
        """
        # Obtener la estructura base del pipeline
        pipeline_dict = self.pipeline.to_dict()

        # Añadir posiciones UI a los nodos (solo si no existen)
        pipeline_dict["pipelineGraph"]["nodes"] = self._add_ui_positions(
            pipeline_dict["pipelineGraph"]["nodes"]
        )

        self._trim_sql_boundary_blank_lines(pipeline_dict["pipelineGraph"]["nodes"])

        if format_pyspark_code:
            self._format_pyspark_code_fields(pipeline_dict["pipelineGraph"]["nodes"])

        # No reordenamos nodos ni edges porque el orden no importa, solo el contenido

        # Construir el JSON completo
        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        settings = deepcopy(self.STANDARD_SETTINGS)
        pipeline_version = getattr(self.pipeline, "version", 0)
        if not isinstance(pipeline_version, int):
            try:
                pipeline_version = int(pipeline_version)
            except (TypeError, ValueError):
                pipeline_version = 0

        rocket_json = {
            "id": self.pipeline.workflow_id or str(uuid.uuid4()),
            "name": self.pipeline.name,
            "description": self.pipeline.description,
            "settings": settings,
            "pipelineGraph": pipeline_dict["pipelineGraph"],
            "executionEngine": self.pipeline.execution_engine.value,
            "workflowType": "SpartaWorkflow",
            "uiSettings": getattr(self.pipeline, "raw_ui_settings", None)
            or {
                "position": {"x": -2083.536303142103, "y": -859.7024044750958, "k": 4.0}
            },
            "creationDate": now,
            "lastUpdateDate": now,
            "version": pipeline_version,
            "readOnly": False,
            "releaseInProgress": False,
            "tags": [],
            "debugMode": False,
            "debugAsExecutionMaybe": False,
            "versionSparta": "3.6.3",
            "normalizedName": self.pipeline.name.lower(),
            "isHybridStreaming": False,
        }

        # Aplicar opciones tipadas de settings (siempre desde atributos del Pipeline)
        global_settings = getattr(self.pipeline, "global_settings", None)
        if global_settings is not None:
            settings_global = (
                global_settings.to_dict()
                if hasattr(global_settings, "to_dict")
                else global_settings
            )
            if isinstance(settings_global, dict):
                rocket_json["settings"]["global"].update(settings_global)

        errors_management = getattr(self.pipeline, "errors_management", None)
        if errors_management is not None:
            settings_errors = (
                errors_management.to_dict()
                if hasattr(errors_management, "to_dict")
                else errors_management
            )
            if isinstance(settings_errors, dict):
                rocket_json["settings"]["errorsManagement"] = settings_errors

        structured_streaming_settings = getattr(
            self.pipeline, "structured_streaming_settings", None
        )
        if structured_streaming_settings is not None:
            settings_structured_streaming = (
                structured_streaming_settings.to_dict()
                if hasattr(structured_streaming_settings, "to_dict")
                else structured_streaming_settings
            )
            if isinstance(settings_structured_streaming, dict):
                rocket_json["settings"][
                    "structuredStreamingSettings"
                ] = settings_structured_streaming

        # Reconstruir campos que siempre deben estar sincronizados con parámetros del decorador
        # (estos campos se filtran de raw_settings para evitar duplicación)

        # 1. parametersLists - reconstruir desde parameters_lists del decorador
        if getattr(self.pipeline, "parameters_lists", None):
            base_lists = rocket_json["settings"]["global"].get("parametersLists", [])
            extra_lists = [p for p in self.pipeline.parameters_lists if p]
            rocket_json["settings"]["global"]["parametersLists"] = list(
                dict.fromkeys(base_lists + extra_lists)
            )

        # 2. parametersUsed - siempre reconstruir desde el código
        rocket_json["settings"]["global"][
            "parametersUsed"
        ] = self._extract_parameters_used()

        # 3. parametersSettings.userDefinedParameters - reconstruir desde params
        rocket_json["settings"]["global"]["parametersSettings"] = {
            "userDefinedParameters": self._build_user_parameters()
        }

        # 4. userPluginsJars - reconstruir desde user_plugins_jars
        if getattr(self.pipeline, "user_plugins_jars", None):
            jars = [j for j in self.pipeline.user_plugins_jars if j]
            rocket_json["settings"]["global"]["userPluginsJars"] = jars
        elif "userPluginsJars" not in rocket_json["settings"]["global"]:
            rocket_json["settings"]["global"]["userPluginsJars"] = []

        # 5. dockerSettings - siempre reconstruir desde STANDARD_SETTINGS (configuración interna)
        if "dockerSettings" not in rocket_json["settings"]["global"]:
            rocket_json["settings"]["global"]["dockerSettings"] = (
                self.STANDARD_SETTINGS["global"]["dockerSettings"]
            )

        # 6. kubernetesDeploymentSettings - siempre reconstruir desde STANDARD_SETTINGS (configuración interna)
        if "kubernetesDeploymentSettings" not in rocket_json["settings"]["global"]:
            rocket_json["settings"]["global"]["kubernetesDeploymentSettings"] = (
                self.STANDARD_SETTINGS["global"]["kubernetesDeploymentSettings"]
            )

        # 7. debugSettings - siempre reconstruir desde STANDARD_SETTINGS (configuración interna)
        if "debugSettings" not in rocket_json["settings"]["global"]:
            rocket_json["settings"]["global"]["debugSettings"] = self.STANDARD_SETTINGS[
                "global"
            ]["debugSettings"]

        # 7.1. sqlSettings - asegurar estructura para sincronizar sentencias y registros
        if "sqlSettings" not in rocket_json["settings"]["global"]:
            rocket_json["settings"]["global"]["sqlSettings"] = deepcopy(
                self.STANDARD_SETTINGS["global"]["sqlSettings"]
            )

        # 8. streamingSettings - siempre reconstruir desde STANDARD_SETTINGS (configuración interna)
        if "streamingSettings" not in rocket_json["settings"]:
            rocket_json["settings"]["streamingSettings"] = self.STANDARD_SETTINGS[
                "streamingSettings"
            ]

        # 9. sparkSettings - siempre reconstruir desde STANDARD_SETTINGS (configuración interna)
        if "sparkSettings" not in rocket_json["settings"]:
            rocket_json["settings"]["sparkSettings"] = deepcopy(
                self.STANDARD_SETTINGS["sparkSettings"]
            )

        # 9.1. pythonEnvDefinition - sincronizar desde parámetro explícito si existe
        python_env_definition = getattr(self.pipeline, "python_env_definition", None)
        if python_env_definition:
            serialized_python_env = self._serialize_python_env_definition(
                python_env_definition
            )
            if serialized_python_env:
                rocket_json["settings"]["pythonEnvDefinition"] = serialized_python_env
        elif "pythonEnvDefinition" not in rocket_json["settings"]:
            rocket_json["settings"]["pythonEnvDefinition"] = deepcopy(
                self.STANDARD_SETTINGS["pythonEnvDefinition"]
            )

        # Si hay userSparkConf personalizado en el pipeline, inyectarlo
        user_spark_conf = getattr(self.pipeline, "user_spark_conf", None)
        if user_spark_conf:
            # Convertir de diccionario a lista de objetos {key, value}
            if isinstance(user_spark_conf, dict):
                user_spark_conf_list = [
                    {"key": key, "value": value}
                    for key, value in user_spark_conf.items()
                ]
                rocket_json["settings"]["sparkSettings"]["sparkConf"][
                    "userSparkConf"
                ] = user_spark_conf_list
            else:
                # Si ya es una lista, usarla directamente
                rocket_json["settings"]["sparkSettings"]["sparkConf"][
                    "userSparkConf"
                ] = user_spark_conf

        # Agregar sentencias SQL de pre-ejecución (siempre sincronizado con el decorador)
        if getattr(self.pipeline, "pre_execution_sql_sentences", None):
            sql_sentences = self._serialize_sql_sentence_items(
                self.pipeline.pre_execution_sql_sentences
            )
            rocket_json["settings"]["global"]["sqlSettings"][
                "preExecutionSqlSentences"
            ] = sql_sentences

        # Agregar sentencias SQL de post-ejecución (siempre sincronizado con el decorador)
        if getattr(self.pipeline, "post_execution_sql_sentences", None):
            sql_sentences = self._serialize_sql_sentence_items(
                self.pipeline.post_execution_sql_sentences
            )
            rocket_json["settings"]["global"]["sqlSettings"][
                "postExecutionSqlSentences"
            ] = sql_sentences

        # Agregar UDFs a registrar (siempre sincronizado con el decorador)
        if getattr(self.pipeline, "udfs_to_register", None):
            udfs = self._serialize_to_register_items(self.pipeline.udfs_to_register)
            rocket_json["settings"]["global"]["sqlSettings"]["udfsToRegister"] = udfs

        # Agregar UDAFs a registrar (siempre sincronizado con el decorador)
        if getattr(self.pipeline, "udafs_to_register", None):
            udafs = self._serialize_to_register_items(self.pipeline.udafs_to_register)
            rocket_json["settings"]["global"]["sqlSettings"]["udafsToRegister"] = udafs

        # Incluir el workflowMasterId si existe
        if getattr(self.pipeline, "asset_id", None):
            rocket_json["workflowMasterId"] = self.pipeline.asset_id

        # Incluir metadatos crudos si existen
        raw_metadata = getattr(self.pipeline, "raw_metadata", None) or {}
        if raw_metadata:
            allowed_keys = {
                "group",
                "groupId",
                "projectId",
                "versionSparta",
                "creationDate",
                "lastUpdateDate",
                "version",
                "readOnly",
                "releaseInProgress",
                "tags",
                "debugMode",
                "debugAsExecutionMaybe",
                "normalizedName",
                "isHybridStreaming",
                "workflowType",
                "workflowMasterId",
            }
            for key, value in raw_metadata.items():
                if key in allowed_keys:
                    rocket_json[key] = value

        # Reconstruir campos de metadata desde parámetros del decorador si no están en raw_metadata
        if (
            getattr(self.pipeline, "project_id", None)
            and "projectId" not in rocket_json
        ):
            rocket_json["projectId"] = self.pipeline.project_id

        if getattr(self.pipeline, "group_id", None) and "groupId" not in rocket_json:
            rocket_json["groupId"] = self.pipeline.group_id

        # Reconstruir campo 'group' desde group_id y group_name si no está en raw_metadata
        if "group" not in rocket_json:
            group_id = getattr(self.pipeline, "group_id", None)
            group_name = getattr(self.pipeline, "group_name", None)
            if group_id or group_name:
                rocket_json["group"] = {}
                if group_id:
                    rocket_json["group"]["id"] = group_id
                if group_name:
                    rocket_json["group"]["name"] = group_name

        # workflowMasterId ya se agregó arriba desde asset_id
        # pero verificamos que esté presente
        if (
            getattr(self.pipeline, "asset_id", None)
            and "workflowMasterId" not in rocket_json
        ):
            rocket_json["workflowMasterId"] = self.pipeline.asset_id

        return rocket_json

    def to_json(self, indent: int = 2, format_pyspark_code: bool = False) -> str:
        """
        Compila y serializa el pipeline a JSON string.

        Args:
            indent: Nivel de indentación para el JSON
            format_pyspark_code: Si formatea pythonCode en nodos PySpark.

        Returns:
            String JSON formateado
        """
        compiled = self.compile(format_pyspark_code=format_pyspark_code)
        return json.dumps(compiled, indent=indent, ensure_ascii=False)

    def save(self, filepath: str, format_pyspark_code: bool = False) -> None:
        """
        Compila y guarda el pipeline en un archivo JSON.

        Args:
            filepath: Ruta donde guardar el archivo JSON
            format_pyspark_code: Si formatea pythonCode en nodos PySpark.
        """
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(self.to_json(format_pyspark_code=format_pyspark_code))
