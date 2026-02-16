"""
Debug options management for py2rocket pipeline steps.

This module provides a structured way to handle debugOptions configuration
that appears in all step types (Input, Transformation, Output).
"""

from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, Any, Literal
import json


MockType = Literal["AutoInfer", "NoMock"]


@dataclass
class DebugOptions:
    """
    Structured representation of debugOptions configuration.

    Based on analysis of docs/ref/*.json files, debugOptions contains:
    - executeStepAutoDebug: Enable automatic debug execution
    - executeStepDebug: Enable debug execution
    - mockType: Type of mocking ("AutoInfer" for Input, "NoMock" for Output/Transformation)

    Default values depend on step type:
    - Input steps: mockType = "AutoInfer"
    - Output/Transformation steps: mockType = "NoMock"
    """

    execute_step_auto_debug: bool = True
    execute_step_debug: bool = True
    mock_type: MockType = "NoMock"

    @classmethod
    def for_input(cls) -> "DebugOptions":
        """Create debug options with defaults for Input steps."""
        return cls(
            execute_step_auto_debug=True, execute_step_debug=True, mock_type="AutoInfer"
        )

    @classmethod
    def for_output(cls) -> "DebugOptions":
        """Create debug options with defaults for Output steps."""
        return cls(
            execute_step_auto_debug=True, execute_step_debug=True, mock_type="NoMock"
        )

    @classmethod
    def for_transformation(cls) -> "DebugOptions":
        """Create debug options with defaults for Transformation steps."""
        return cls(
            execute_step_auto_debug=True, execute_step_debug=True, mock_type="NoMock"
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DebugOptions":
        """Create DebugOptions from a dictionary (camelCase or snake_case)."""
        # Handle both camelCase (from JSON) and snake_case (from Python)
        return cls(
            execute_step_auto_debug=data.get(
                "executeStepAutoDebug", data.get("execute_step_auto_debug", True)
            ),
            execute_step_debug=data.get(
                "executeStepDebug", data.get("execute_step_debug", True)
            ),
            mock_type=data.get("mockType", data.get("mock_type", "NoMock")),
        )

    @classmethod
    def from_json_string(cls, json_str: str) -> "DebugOptions":
        """Parse debugOptions from JSON string (as stored in Rocket JSON)."""
        if not json_str or json_str.strip() == "":
            return cls()

        try:
            data = json.loads(json_str)
            return cls.from_dict(data)
        except json.JSONDecodeError:
            # Fallback to defaults if JSON is malformed
            return cls()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with camelCase keys (for JSON serialization)."""
        return {
            "executeStepAutoDebug": self.execute_step_auto_debug,
            "executeStepDebug": self.execute_step_debug,
            "mockType": self.mock_type,
        }

    def to_json_string(self) -> str:
        """Convert to JSON string (as stored in Rocket JSON)."""
        return json.dumps(self.to_dict())

    def to_config_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for node configuration."""
        return self.to_dict()

    def merge_with(self, other: Optional["DebugOptions"]) -> "DebugOptions":
        """Merge with another DebugOptions, preferring non-default values from other."""
        if other is None:
            return self

        return DebugOptions(
            execute_step_auto_debug=other.execute_step_auto_debug,
            execute_step_debug=other.execute_step_debug,
            mock_type=other.mock_type,
        )


def get_default_debug_options(step_type: str) -> DebugOptions:
    """
    Get default debug options for a given step type.

    Args:
        step_type: "Input", "Transformation", or "Output"

    Returns:
        DebugOptions with appropriate defaults
    """
    step_type_lower = step_type.lower()

    if step_type_lower == "input":
        return DebugOptions.for_input()
    elif step_type_lower == "transformation":
        return DebugOptions.for_transformation()
    elif step_type_lower == "output":
        return DebugOptions.for_output()
    else:
        # Default to transformation-style
        return DebugOptions.for_transformation()
