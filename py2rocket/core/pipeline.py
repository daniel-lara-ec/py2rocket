"""
DSL Classes para Stratio Rocket Pipeline Generator

Este módulo define las clases fundamentales para construir pipelines de Stratio Rocket
de forma declarativa. El DSL NO ejecuta datos, solo DESCRIBE un DAG que será
ejecutado por Rocket.

Arquitectura:
    Notebook (exploración) → DSL Python → IR → Compiler → JSON Rocket → Stratio Rocket
"""

from typing import List, Dict, Any, Optional, Literal
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
    INVALID_DATA = "InvalidData"


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

    def to_dict(self) -> Dict[str, Any]:
        """Convierte el nodo a formato diccionario para serialización JSON"""
        return {
            "name": self.name,
            "stepType": self.step_type.value,
            "className": self.class_name,
            "classPrettyName": self.class_pretty_name,
            "arity": self.arity,
            "description": self.description,
            "configuration": {
                "priority": str(self.priority),
                "debugOptions": '{"executeStepAutoDebug":true}',
                **self.configuration,
            },
            "supportedEngines": self.supported_engines,
            "executionEngine": self.execution_engine.value,
            "supportedDataRelations": self.supported_data_relations,
            "lineageProperties": [],
            "outputsWriter": self.outputs_writer,
        }


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
    project_id: Optional[str] = None
    group_id: Optional[str] = None
    asset_id: Optional[str] = None
    parameters_lists: List[str] = field(default_factory=list)
    pre_execution_sql_sentences: List[str] = field(default_factory=list)
    udfs_to_register: List[str] = field(default_factory=list)
    udafs_to_register: List[str] = field(default_factory=list)
    user_spark_conf: Dict[str, str] = field(default_factory=dict)

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
                "annotations": [],
                "nodeGroups": [],
            },
            "parameters": self.parameters,
        }


class StepResult:
    """
    Representa el resultado de un paso en el flujo.

    Se usa para encadenar operaciones y construir el DAG automáticamente.
    Cada operación retorna un StepResult que puede ser usado como input
    en operaciones subsecuentes.

    Attributes:
        node: El nodo asociado a este resultado
        pipeline: Referencia al pipeline que contiene este nodo
    """

    def __init__(self, node: Node, pipeline: Pipeline):
        self.node = node
        self.pipeline = pipeline

    def __repr__(self):
        return f"StepResult({self.node.name})"

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
