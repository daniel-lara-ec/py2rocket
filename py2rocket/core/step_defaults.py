from pathlib import Path
from typing import Optional, Dict, Any, Tuple
import json as jsonlib


_REF_DEFAULTS_CACHE: Optional[Dict[str, Dict[str, Any]]] = None
_REF_DEFAULTS_BY_PRETTY: Optional[Dict[Tuple[str, str], Dict[str, Any]]] = None


def _load_ref_defaults() -> (
    Tuple[Dict[str, Dict[str, Any]], Dict[Tuple[str, str], Dict[str, Any]]]
):
    """Carga defaults de steps desde docs/ref/*.json si existen."""
    global _REF_DEFAULTS_CACHE, _REF_DEFAULTS_BY_PRETTY
    if _REF_DEFAULTS_CACHE is not None and _REF_DEFAULTS_BY_PRETTY is not None:
        return _REF_DEFAULTS_CACHE, _REF_DEFAULTS_BY_PRETTY

    defaults: Dict[str, Dict[str, Any]] = {}
    by_pretty: Dict[Tuple[str, str], Dict[str, Any]] = {}

    try:
        ref_dir = Path(__file__).resolve().parents[2] / "docs" / "ref"
        if ref_dir.exists():
            for ref_file in ref_dir.glob("*.json"):
                try:
                    data = jsonlib.loads(ref_file.read_text(encoding="utf-8"))
                except Exception:
                    continue

                class_name = data.get("className")
                if class_name:
                    defaults[class_name] = data

                step_type = data.get("stepType")
                class_pretty_name = data.get("classPrettyName")
                if step_type and class_pretty_name:
                    by_pretty[(str(step_type), str(class_pretty_name))] = data
    except Exception:
        defaults = {}
        by_pretty = {}

    _REF_DEFAULTS_CACHE = defaults
    _REF_DEFAULTS_BY_PRETTY = by_pretty
    return defaults, by_pretty


def _get_step_defaults(
    class_name: Optional[str] = None,
    step_type: Optional[Any] = None,
    class_pretty_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Obtiene defaults por class_name o (step_type, class_pretty_name)."""
    defaults, by_pretty = _load_ref_defaults()

    if class_name and class_name in defaults:
        return defaults[class_name]

    if step_type and class_pretty_name:
        key = (str(step_type), str(class_pretty_name))
        return by_pretty.get(key)

    return None
