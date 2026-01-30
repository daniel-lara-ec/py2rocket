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
    TRANSFORM = "Transform"
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
        execution_engine: Motor de ejecución (Batch, Streaming, Hybrid)
        priority: Prioridad de ejecución (menor = antes)
        configuration: Configuración específica del nodo
        supported_engines: Motores compatibles con este nodo
        supported_data_relations: Relaciones de datos soportadas
    """

    name: str
    step_type: StepType
    class_name: str
    class_pretty_name: str
    execution_engine: ExecutionEngine = ExecutionEngine.HYBRID
    priority: int = 50
    configuration: Dict[str, Any] = field(default_factory=dict)
    supported_engines: List[str] = field(default_factory=lambda: ["Batch", "Hybrid"])
    supported_data_relations: List[str] = field(default_factory=lambda: ["ValidData"])
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convierte el nodo a formato diccionario para serialización JSON"""
        return {
            "name": self.name,
            "stepType": self.step_type.value,
            "className": self.class_name,
            "classPrettyName": self.class_pretty_name,
            "arity": (
                ["NullaryToNary"]
                if self.step_type == StepType.INPUT
                else ["NaryToNullary"]
            ),
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
            "outputsWriter": [],
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
        """
        # TODO: Implementar validaciones de DAG
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
