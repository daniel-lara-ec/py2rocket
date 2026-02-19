"""
DSL Classes para Stratio Rocket Pipeline Generator

Este módulo define las clases fundamentales para construir pipelines de Stratio Rocket
de forma declarativa. El DSL NO ejecuta datos, solo DESCRIBE un DAG que será
ejecutado por Rocket.

Arquitectura:
    Notebook (exploración) → DSL Python → IR → Compiler → JSON Rocket → Stratio Rocket
"""

from typing import List, Dict, Any, Optional, Literal, Union
from dataclasses import dataclass, field
from enum import Enum


class ExecutionEngine(Enum):
    """Motor de ejecución del pipeline"""

    BATCH = "Batch"
    STREAMING = "Streaming"
    HYBRID = "Hybrid"


class StepType(Enum):
    """Tipo de paso en el pipeline"""

    INPUT = "Input"
    TRANSFORMATION = "Transformation"
    OUTPUT = "Output"


class DataRelation(Enum):
    """Tipo de relación de datos entre nodos"""

    VALID_DATA = "ValidData"
    INVALID_DATA = "DiscardedData"


@dataclass
class OutputWriter:
    """
    Configuración de escritura para outputs desde un transformation.

    Este objeto representa la configuración outputsWriter que se adjunta
    al nodo transformation y controla cómo se escriben los datos en el output.
    """

    output_step_name: str
    save_mode: str = "Overwrite"
    table_name: str = ""
    discard_table_name: str = ""
    partition_by: Optional[str] = None
    partition_overwrite: bool = True
    check_if_empty: bool = False
    partition_columns: str = ""
    partitions: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convierte el OutputWriter a formato dict para JSON"""
        extra_options = {
            "partitionBy": self.partition_by if self.partition_by else "overwrite",
            "partitionOverwriteEnabled": self.partition_overwrite,
            "checkIfEmpty": self.check_if_empty,
            "partitionColumns": self.partition_columns,
            "saveMode": self.save_mode,
            "partitions": self.partitions,
        }

        return {
            "saveMode": self.save_mode,
            "outputStepName": self.output_step_name,
            "tableName": self.table_name,
            "discardTableName": self.discard_table_name,
            "extraOptions": extra_options,
        }


@dataclass
class SqlSentence:
    """Representa una sentencia SQL para sqlSettings."""

    sentence: str

    def to_dict(self) -> Dict[str, Any]:
        """Convierte la sentencia a formato dict para JSON."""
        return {"sentence": self.sentence}


@dataclass
class ToRegister:
    """Representa un UDF/UDAF a registrar en sqlSettings."""

    name: str

    def to_dict(self) -> Dict[str, Any]:
        """Convierte el registro a formato dict para JSON."""
        return {"name": self.name}


@dataclass
class PythonEnvDefinition:
    """Representa la configuración pythonEnvDefinition del workflow."""

    v_env_management_mode: str = "DefaultExecutionVirtualEnv"
    conda_yaml_definition: str = ""
    freeze_after_debug: bool = False
    conda_pack_extension: List[Any] = field(default_factory=list)
    execute_conda_unpack_after_activate: bool = False
    py_spark_native_extensions: List[Any] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convierte el objeto a formato dict compatible con Rocket JSON."""
        return {
            "vEnvManagementMode": self.v_env_management_mode,
            "condaYamlDefinition": self.conda_yaml_definition,
            "freezeAfterDebug": self.freeze_after_debug,
            "condaPackExtension": self.conda_pack_extension,
            "executeCondaUnpackAfterActivate": self.execute_conda_unpack_after_activate,
            "pySparkNativeExtensions": self.py_spark_native_extensions,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PythonEnvDefinition":
        """Construye un PythonEnvDefinition desde un dict de Rocket JSON."""
        if not isinstance(data, dict):
            return cls()

        return cls(
            v_env_management_mode=data.get(
                "vEnvManagementMode", "DefaultExecutionVirtualEnv"
            ),
            conda_yaml_definition=data.get("condaYamlDefinition", ""),
            freeze_after_debug=data.get("freezeAfterDebug", False),
            conda_pack_extension=data.get("condaPackExtension", []) or [],
            execute_conda_unpack_after_activate=data.get(
                "executeCondaUnpackAfterActivate", False
            ),
            py_spark_native_extensions=data.get("pySparkNativeExtensions", []) or [],
        )


@dataclass
class AutoDebugSettings:
    """Representa settings.global.autoDebugSettings."""

    enable_auto_debug: bool = True
    force_auto_debug_execution_for_all_steps: bool = False
    do_not_use_cache_data: bool = True
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "enableAutoDebug": self.enable_auto_debug,
            "forceAutoDebugExecutionForAllSteps": self.force_auto_debug_execution_for_all_steps,
            "doNotUseCacheData": self.do_not_use_cache_data,
        }
        data.update(self.extra)
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AutoDebugSettings":
        if not isinstance(data, dict):
            return cls()
        known = {
            "enableAutoDebug",
            "forceAutoDebugExecutionForAllSteps",
            "doNotUseCacheData",
        }
        extra = {k: v for k, v in data.items() if k not in known}
        return cls(
            enable_auto_debug=data.get("enableAutoDebug", True),
            force_auto_debug_execution_for_all_steps=data.get(
                "forceAutoDebugExecutionForAllSteps", False
            ),
            do_not_use_cache_data=data.get("doNotUseCacheData", True),
            extra=extra,
        )


@dataclass
class ExecutionMetricsSettings:
    """Representa settings.global.executionMetricsSettings."""

    custom_metric_labels: List[Any] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        data = {"customMetricLabels": self.custom_metric_labels}
        data.update(self.extra)
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutionMetricsSettings":
        if not isinstance(data, dict):
            return cls()
        known = {"customMetricLabels"}
        extra = {k: v for k, v in data.items() if k not in known}
        return cls(
            custom_metric_labels=data.get("customMetricLabels", []) or [],
            extra=extra,
        )


@dataclass
class GlobalSettings:
    """Representa settings.global para opciones extra preservadas."""

    execution_mode: str = "kubernetes"
    enable_quality_rules: bool = True
    auto_debug_settings: AutoDebugSettings = field(default_factory=AutoDebugSettings)
    get_total_rows_by_step: bool = False
    enable_project_env_var: bool = True
    execution_metrics_settings: ExecutionMetricsSettings = field(
        default_factory=ExecutionMetricsSettings
    )
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "executionMode": self.execution_mode,
            "enableQualityRules": self.enable_quality_rules,
            "autoDebugSettings": self.auto_debug_settings.to_dict(),
            "getTotalRowsByStep": self.get_total_rows_by_step,
            "enableProjectEnvVar": self.enable_project_env_var,
            "executionMetricsSettings": self.execution_metrics_settings.to_dict(),
        }
        data.update(self.extra)
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GlobalSettings":
        if not isinstance(data, dict):
            return cls()
        known = {
            "executionMode",
            "enableQualityRules",
            "autoDebugSettings",
            "getTotalRowsByStep",
            "enableProjectEnvVar",
            "executionMetricsSettings",
        }
        extra = {k: v for k, v in data.items() if k not in known}
        return cls(
            execution_mode=data.get("executionMode", "kubernetes"),
            enable_quality_rules=data.get("enableQualityRules", True),
            auto_debug_settings=AutoDebugSettings.from_dict(
                data.get("autoDebugSettings", {})
            ),
            get_total_rows_by_step=data.get("getTotalRowsByStep", False),
            enable_project_env_var=data.get("enableProjectEnvVar", True),
            execution_metrics_settings=ExecutionMetricsSettings.from_dict(
                data.get("executionMetricsSettings", {})
            ),
            extra=extra,
        )


@dataclass
class GenericErrorManagement:
    """Representa settings.errorsManagement.genericErrorManagement."""

    when_error: str = "Error"
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        data = {"whenError": self.when_error}
        data.update(self.extra)
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GenericErrorManagement":
        if not isinstance(data, dict):
            return cls()
        known = {"whenError"}
        extra = {k: v for k, v in data.items() if k not in known}
        return cls(when_error=data.get("whenError", "Error"), extra=extra)


@dataclass
class ErrorsManagement:
    """Representa settings.errorsManagement."""

    generic_error_management: GenericErrorManagement = field(
        default_factory=GenericErrorManagement
    )
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        data = {"genericErrorManagement": self.generic_error_management.to_dict()}
        data.update(self.extra)
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ErrorsManagement":
        if not isinstance(data, dict):
            return cls()
        known = {"genericErrorManagement"}
        extra = {k: v for k, v in data.items() if k not in known}
        return cls(
            generic_error_management=GenericErrorManagement.from_dict(
                data.get("genericErrorManagement", {})
            ),
            extra=extra,
        )


@dataclass
class StructuredStreamingSettings:
    """Representa settings.structuredStreamingSettings."""

    settings: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return self.settings

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StructuredStreamingSettings":
        if not isinstance(data, dict):
            return cls()
        return cls(settings=data)


@dataclass
class UIPosition:
    """Posición de un nodo en la interfaz gráfica de Rocket.

    Attributes:
        x: Coordenada X en el canvas (entero)
        y: Coordenada Y en el canvas (entero)
    """

    x: int
    y: int

    def to_dict(self) -> Dict[str, Any]:
        """Convierte la posición a formato para uiConfiguration."""
        return {"position": {"x": self.x, "y": self.y}}

    @classmethod
    def from_dict(cls, ui_config: Dict[str, Any]) -> Optional["UIPosition"]:
        """Crea una UIPosition desde uiConfiguration."""
        if not ui_config:
            return None
        pos = ui_config.get("position")
        if not pos or "x" not in pos or "y" not in pos:
            return None
        # Redondear coordenadas a enteros
        return cls(x=round(pos["x"]), y=round(pos["y"]))

    def __repr__(self) -> str:
        return f"UIPosition(x={self.x}, y={self.y})"


@dataclass
class Node:
    """
    Representa un nodo en el DAG del pipeline.

    Cada nodo es una operación (Input, Transform, Output) que procesa datos.
    Los nodos se conectan mediante edges para formar el flujo de datos.

    Attributes:
        name: Identificador único del nodo en el pipeline
        step_type: Tipo de paso (Input, Transform, Output)
        class_name: Nombre de la clase Rocket que implementa este paso
        class_pretty_name: Nombre legible de la clase
        arity: Lista de relaciones de aridad (ej: ["NullaryToNary"], ["NaryToNary"])
        execution_engine: Motor de ejecución (Batch, Streaming, Hybrid)
        priority: Prioridad de ejecución (menor = antes)
        configuration: Configuración específica del nodo
        supported_engines: Motores compatibles con este nodo
        supported_data_relations: Relaciones de datos soportadas
        outputs_writer: Configuración de outputsWriter para el nodo
    """

    name: str
    step_type: StepType
    class_name: str
    class_pretty_name: str
    arity: List[str] = field(default_factory=list)
    execution_engine: ExecutionEngine = ExecutionEngine.HYBRID
    priority: int = 50
    configuration: Dict[str, Any] = field(default_factory=dict)
    supported_engines: List[str] = field(default_factory=lambda: ["Batch", "Hybrid"])
    supported_data_relations: List[str] = field(default_factory=lambda: ["ValidData"])
    description: str = ""
    outputs_writer: List[Dict[str, Any]] = field(default_factory=list)
    ui_configuration: Optional[Dict[str, Any]] = None
    lineage_properties: List[Any] = field(default_factory=list)
    last_modified: Optional[str] = None
    include_debug_options: bool = True
    include_supported_data_relations: bool = True
    include_description: bool = True
    include_node_metadata: bool = True  # Control de serialización de NodeMetadata
    node_metadata: Optional[Any] = None  # NodeMetadata object

    def to_dict(self) -> Dict[str, Any]:
        """Convierte el nodo a formato diccionario para serialización JSON"""
        config = {
            "priority": str(self.priority),
            **self.configuration,
        }

        # Asegurar que debugOptions sea una cadena JSON, no un dict
        if "debugOptions" in config and isinstance(config["debugOptions"], dict):
            import json

            config["debugOptions"] = json.dumps(config["debugOptions"])

        if self.include_debug_options and "debugOptions" not in config:
            # Agregar debugOptions por defecto basándose en el tipo de paso
            if self.step_type == StepType.INPUT:
                config["debugOptions"] = (
                    '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"AutoInfer"}'
                )
            else:  # Transformation or Output
                config["debugOptions"] = (
                    '{"executeStepAutoDebug":true,"executeStepDebug":true,"mockType":"NoMock"}'
                )

        # Agregar NodeMetadata (isSaved, genAI*) si include_node_metadata es True
        if self.include_node_metadata:
            # Si no hay node_metadata, crear defaults
            if self.node_metadata is None:
                from py2rocket.core.node_metadata import NodeMetadata

                # Determinar si es Trigger para incluir genAIMetadataTablesDescription
                is_trigger = self.class_name == "TriggerTransformStep"
                if self.step_type == StepType.INPUT:
                    self.node_metadata = NodeMetadata.for_input()
                elif self.step_type == StepType.TRANSFORMATION:
                    self.node_metadata = NodeMetadata.for_transformation(
                        is_trigger=is_trigger
                    )
                else:  # Output
                    self.node_metadata = NodeMetadata.for_output()

            # Agregar a config
            metadata_dict = self.node_metadata.to_config_dict()
            config.update(metadata_dict)

        node_dict = {
            "name": self.name,
            "stepType": self.step_type.value,
            "className": self.class_name,
            "classPrettyName": self.class_pretty_name,
            "arity": self.arity,
            "description": self.description,
            "configuration": config,
            "supportedEngines": self.supported_engines,
            "executionEngine": self.execution_engine.value,
            "supportedDataRelations": self.supported_data_relations,
            "lineageProperties": self.lineage_properties,
            "outputsWriter": self.outputs_writer,
        }

        if not self.include_supported_data_relations:
            node_dict.pop("supportedDataRelations", None)
        if not self.include_description:
            node_dict.pop("description", None)

        if self.ui_configuration is not None:
            node_dict["uiConfiguration"] = self.ui_configuration
        if self.last_modified:
            node_dict["lastModified"] = self.last_modified

        return node_dict


@dataclass
class Edge:
    """
    Representa una conexión entre dos nodos del DAG.

    Define el flujo de datos desde un nodo origen a un nodo destino.

    Attributes:
        origin: Nombre del nodo origen
        destination: Nombre del nodo destino
        data_type: Tipo de datos que fluyen (ValidData, InvalidData)
    """

    origin: str
    destination: str
    data_type: DataRelation = DataRelation.VALID_DATA

    def to_dict(self) -> Dict[str, Any]:
        """Convierte el edge a formato diccionario para serialización JSON"""
        return {
            "origin": self.origin,
            "destination": self.destination,
            "dataType": self.data_type.value,
        }


@dataclass
class Pipeline:
    """
    Representa un pipeline completo de Stratio Rocket.

    Un pipeline es un DAG (Directed Acyclic Graph) que define el flujo de
    procesamiento de datos. Contiene nodos (operaciones) conectados por
    edges (flujo de datos).

    Attributes:
        name: Nombre único del pipeline
        execution_engine: Motor de ejecución del pipeline
        nodes: Lista de nodos que componen el pipeline
        edges: Lista de conexiones entre nodos
        parameters: Parámetros de negocio del pipeline
        description: Descripción del propósito del pipeline
        project_id: UUID del proyecto (obtenido de la API)
        group_id: UUID del grupo (obtenido de la API)
        asset_id: UUID del asset creado en Rocket

    Reglas:
        - NO se permiten ciclos en el DAG
        - NO se permiten nodos huérfanos (sin conexiones)
        - NO se permiten outputs sin inputs
        - Cada variable (nodo) debe ser única
    """

    name: str
    execution_engine: ExecutionEngine = ExecutionEngine.HYBRID
    nodes: List[Node] = field(default_factory=list)
    edges: List[Edge] = field(default_factory=list)
    parameters: Dict[str, str] = field(default_factory=dict)
    description: str = ""
    workflow_id: Optional[str] = None
    version: int = 0
    project_id: Optional[str] = None
    group_id: Optional[str] = None
    group_name: Optional[str] = None
    asset_id: Optional[str] = None
    parameters_lists: List[str] = field(default_factory=list)
    pre_execution_sql_sentences: List[Union[str, SqlSentence, Dict[str, Any]]] = field(
        default_factory=list
    )
    post_execution_sql_sentences: List[Union[str, SqlSentence, Dict[str, Any]]] = field(
        default_factory=list
    )
    udfs_to_register: List[Union[str, ToRegister, Dict[str, Any]]] = field(
        default_factory=list
    )
    udafs_to_register: List[Union[str, ToRegister, Dict[str, Any]]] = field(
        default_factory=list
    )
    user_spark_conf: Union[Dict[str, str], List[Dict[str, str]]] = field(
        default_factory=dict
    )
    python_env_definition: Optional[Union[PythonEnvDefinition, Dict[str, Any]]] = None
    global_settings: GlobalSettings = field(default_factory=GlobalSettings)
    errors_management: ErrorsManagement = field(default_factory=ErrorsManagement)
    structured_streaming_settings: StructuredStreamingSettings = field(
        default_factory=StructuredStreamingSettings
    )
    plugins: List[str] = field(default_factory=list)
    user_plugins_jars: List[Dict[str, str]] = field(default_factory=list)
    raw_ui_settings: Optional[Dict[str, Any]] = None
    raw_metadata: Dict[str, Any] = field(default_factory=dict)
    annotations: List[Any] = field(default_factory=list)
    node_groups: List[Any] = field(default_factory=list)

    def add_node(self, node: Node) -> None:
        """Añade un nodo al pipeline"""
        if any(n.name == node.name for n in self.nodes):
            raise ValueError(f"El nodo '{node.name}' ya existe en el pipeline")
        self.nodes.append(node)

    def add_edge(self, edge: Edge) -> None:
        """Añade una conexión entre nodos"""
        # Validar que los nodos existen
        origin_exists = any(n.name == edge.origin for n in self.nodes)
        dest_exists = any(n.name == edge.destination for n in self.nodes)

        if not origin_exists:
            raise ValueError(f"El nodo origen '{edge.origin}' no existe")
        if not dest_exists:
            raise ValueError(f"El nodo destino '{edge.destination}' no existe")

        self.edges.append(edge)

    def validate(self) -> bool:
        """
        Valida que el pipeline cumpla con las reglas del DSL:
        - No ciclos
        - No nodos huérfanos
        - DAG válido
        - Todos los inputs tienen al menos una conexión
        - Todos los transforms tienen entrada y salida
        """
        # Validar que todos los nodos INPUT tengan al menos una conexión saliente
        for node in self.nodes:
            if node.step_type == StepType.INPUT:
                has_connection = any(edge.origin == node.name for edge in self.edges)
                if not has_connection:
                    raise ValueError(
                        f"El nodo INPUT '{node.name}' no tiene conexiones. "
                        f"Todos los inputs deben conectarse a al menos una transformación u output."
                    )

        # Validar que todos los nodos TRANSFORMATION tengan al menos una entrada y una salida
        for node in self.nodes:
            if node.step_type == StepType.TRANSFORMATION:
                has_incoming = any(edge.destination == node.name for edge in self.edges)
                has_outgoing = any(edge.origin == node.name for edge in self.edges)

                if not has_incoming:
                    raise ValueError(
                        f"El nodo TRANSFORMATION '{node.name}' no tiene conexiones de entrada. "
                        f"Todas las transformaciones deben recibir datos de un input u otra transformación."
                    )
                if not has_outgoing:
                    raise ValueError(
                        f"El nodo TRANSFORMATION '{node.name}' no tiene conexiones de salida. "
                        f"Todas las transformaciones deben conectarse a otra transformación u output."
                    )

        # TODO: Implementar validaciones adicionales (ciclos, nodos huérfanos completos)
        return True

    def to_dict(self) -> Dict[str, Any]:
        """Convierte el pipeline completo a formato diccionario"""
        return {
            "name": self.name,
            "description": self.description,
            "executionEngine": self.execution_engine.value,
            "pipelineGraph": {
                "nodes": [node.to_dict() for node in self.nodes],
                "edges": [edge.to_dict() for edge in self.edges],
                "annotations": self.annotations,
                "nodeGroups": self.node_groups,
            },
            "parameters": self.parameters,
        }


class StepResultOutput:
    """
    Representa una salida específica (VALID o DISCARDED) de un nodo multi-output.

    Se usa cuando un nodo puede generar múltiples tipos de datos (ej: Filter con ValidData y DiscardedData).
    Almacena el tipo de relación de datos para crear edges con el dataType correcto.

    Attributes:
        node: El nodo asociado
        pipeline: Referencia al pipeline
        data_relation: Tipo de relación de datos (VALID_DATA o INVALID_DATA)
    """

    def __init__(
        self,
        node: Node,
        pipeline: Pipeline,
        data_relation: DataRelation = DataRelation.VALID_DATA,
    ):
        self.node = node
        self.pipeline = pipeline
        self.data_relation = data_relation

    def __repr__(self):
        return f"StepResultOutput({self.node.name}, {self.data_relation.value})"


class StepResult:
    """
    Representa el resultado de un paso en el flujo.

    Se usa para encadenar operaciones y construir el DAG automáticamente.
    Cada operación retorna un StepResult que puede ser usado como input
    en operaciones subsecuentes.

    Por defecto se comporta como VALID_DATA. Usa la propiedad .discarded
    para obtener la salida DISCARDED_DATA (si el nodo lo soporta).

    Attributes:
        node: El nodo asociado a este resultado
        pipeline: Referencia al pipeline que contiene este nodo
    """

    def __init__(self, node: Node, pipeline: Pipeline):
        self.node = node
        self.pipeline = pipeline
        self.data_relation = DataRelation.VALID_DATA  # Por defecto VALID_DATA

    def __repr__(self):
        return f"StepResult({self.node.name})"

    @property
    def discarded(self) -> StepResultOutput:
        """
        Retorna una salida DISCARDED_DATA del mismo nodo.

        Uso:
            filtro = filter(name="Filter", filter_exp="...", inputs=datos)
            valid_data = filtro           # VALID_DATA por defecto
            invalid_data = filtro.discarded  # DISCARDED_DATA explícito

        Returns:
            StepResultOutput con data_relation = INVALID_DATA
        """
        return StepResultOutput(self.node, self.pipeline, DataRelation.INVALID_DATA)

    def set_outputs_writer(
        self,
        save_mode: str = "Overwrite",
        table_name: str = "",
        discard_table_name: str = "",
        check_if_empty: bool = False,
        partition_by: Optional[List[str]] = None,
        partition_overwrite_enabled: bool = True,
        partition_columns: Optional[List[str]] = None,
        partitions: Optional[int] = None,
    ) -> "StepResult":
        """
        Configura outputsWriter en el nodo asociado sin requerir el outputStepName.

        El outputStepName se completará automáticamente cuando se conecte
        este nodo a un paso Output.

        Args:
            save_mode: Modo de guardado (Overwrite, Append, Ignore, Error)
            table_name: Nombre de tabla destino
            discard_table_name: Nombre de tabla de descartes
            check_if_empty: Si validar vacío antes de escribir
            partition_by: Columnas para particionar (lista de strings)
            partition_overwrite_enabled: Si habilitar overwrite de particiones
            partition_columns: Columnas de partición (lista de strings)
            partitions: Número de particiones

        Returns:
            El mismo StepResult para encadenamiento
        """
        partition_by_str = ",".join(partition_by) if partition_by else "overwrite"
        partition_columns_str = ",".join(partition_columns) if partition_columns else ""
        partitions_str = "" if partitions is None else str(partitions)

        extra_options = {
            "checkIfEmpty": check_if_empty,
            "partitionBy": partition_by_str,
            "partitionOverwriteEnabled": partition_overwrite_enabled,
            "partitionColumns": partition_columns_str,
            "saveMode": save_mode,
            "partitions": partitions_str,
        }

        entry = {
            "saveMode": save_mode,
            "outputStepName": None,
            "tableName": table_name,
            "discardTableName": discard_table_name,
            "extraOptions": extra_options,
        }

        if self.node.outputs_writer is None:
            self.node.outputs_writer = []

        self.node.outputs_writer.append(entry)
        return self
