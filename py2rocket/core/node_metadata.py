"""
Manejo estructurado de metadatos de nodos (GenAI y persistencia).

Gestiona los metadatos relacionados con IA generativa y estado de persistencia
que aparecen en la configuración de los nodos.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class NodeMetadata:
    """
    Metadatos de nodo para GenAI y persistencia.

    Attributes:
        is_saved: Indica si el nodo está guardado (default: True)
        gen_ai_table_description: Descripción de tabla generada por IA (default: "")
        gen_ai_columns: Descripción de columnas generada por IA (default: "")
        gen_ai_tables_description: Descripción de múltiples tablas (solo Trigger, default: "")
    """

    is_saved: bool = True
    gen_ai_table_description: str = ""
    gen_ai_columns: str = ""
    gen_ai_tables_description: Optional[str] = None  # Solo para Trigger

    @staticmethod
    def for_input() -> "NodeMetadata":
        """Crear metadatos con defaults para Input."""
        return NodeMetadata()

    @staticmethod
    def for_transformation(is_trigger: bool = False) -> "NodeMetadata":
        """
        Crear metadatos con defaults para Transformation.

        Args:
            is_trigger: Si True, incluye genAIMetadataTablesDescription
        """
        if is_trigger:
            return NodeMetadata(gen_ai_tables_description="")
        return NodeMetadata()

    @staticmethod
    def for_output() -> "NodeMetadata":
        """Crear metadatos con defaults para Output."""
        return NodeMetadata()

    def is_default(self) -> bool:
        """
        Verifica si el objeto tiene todos los valores por defecto.

        Returns:
            True si todos los valores son los defaults
        """
        if not self.is_saved:
            return False
        if self.gen_ai_table_description:
            return False
        if self.gen_ai_columns:
            return False
        if self.gen_ai_tables_description:
            return False
        return True

    def to_config_dict(self) -> dict:
        """
        Convierte a diccionario para config_override en JSON.

        Returns:
            Diccionario con claves en camelCase
        """
        config = {"isSaved": self.is_saved}

        # Solo agregar genAI fields si no están en default
        if self.gen_ai_table_description != "":
            config["genAIMetadataTableDescription"] = self.gen_ai_table_description
        else:
            config["genAIMetadataTableDescription"] = ""

        if self.gen_ai_columns != "":
            config["genAIMetadataColumns"] = self.gen_ai_columns
        else:
            config["genAIMetadataColumns"] = ""

        # Para Trigger, incluir genAIMetadataTablesDescription
        if self.gen_ai_tables_description is not None:
            if self.gen_ai_tables_description != "":
                config["genAIMetadataTablesDescription"] = (
                    self.gen_ai_tables_description
                )
            else:
                config["genAIMetadataTablesDescription"] = ""

        return config
