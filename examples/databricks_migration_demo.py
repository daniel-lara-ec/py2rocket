"""Demo interactiva para migrar workflows de Rocket a Databricks.

Reutiliza ``get_projects``, ``py2rocket sync`` y ``build_databricks`` para
seguir el mismo proceso público que documenta la librería.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from dotenv import load_dotenv

from py2rocket import build_databricks, get_projects


Project = Dict[str, Any]
DEFAULT_TEMPLATE_NODES = (
    "Parametros",
    "tri_punto_control",
    "tri_registrar_fin",
    "tri_registrar_inicio",
    "sql_rangos_fechas",
    "tri_resumen_ejecucion",
    "pys_notificaciones_ini_tpl",
    "pys_notificaciones_fin_tpl",
)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def template_replacement_from_env() -> Dict[str, Any]:
    raw_nodes = os.getenv(
        "PY2ROCKET_TEMPLATE_NODES", ",".join(DEFAULT_TEMPLATE_NODES)
    )
    return {
        "enabled": _env_bool("PY2ROCKET_TEMPLATE_REPLACEMENT", True),
        "nodes": [name.strip() for name in raw_nodes.split(",") if name.strip()],
        "parameter_node": os.getenv(
            "PY2ROCKET_TEMPLATE_PARAMETER_NODE", "Parametros"
        ),
        "table_field": os.getenv(
            "PY2ROCKET_TEMPLATE_TABLE_FIELD", "tablaUbicacion"
        ),
        "output_name": os.getenv(
            "PY2ROCKET_TEMPLATE_OUTPUT_NAME", "Save_Migrated_Table"
        ),
        "save_mode": os.getenv("PY2ROCKET_TEMPLATE_SAVE_MODE", "Overwrite"),
        "source_node": os.getenv("PY2ROCKET_TEMPLATE_SOURCE_NODE") or None,
    }


def _project_label(project: Project) -> str:
    name = project.get("name") or "Sin nombre"
    normalized_name = project.get("normalizedName") or "sin ruta"
    return f"{name} ({normalized_name})"


def select_project(
    projects: Sequence[Project], requested: Optional[str] = None
) -> Project:
    """Selecciona un proyecto por índice, ID, nombre o nombre normalizado."""
    if not projects:
        raise ValueError("Rocket no devolvió proyectos disponibles")

    if requested:
        requested_folded = requested.strip().casefold()
        for project in projects:
            candidates = (
                project.get("id"),
                project.get("name"),
                project.get("normalizedName"),
            )
            if any(
                str(candidate).casefold() == requested_folded
                for candidate in candidates
                if candidate
            ):
                return project
        raise ValueError(f"No se encontró el proyecto {requested!r}")

    print("\nProyectos disponibles:")
    for index, project in enumerate(projects, start=1):
        print(f"  {index}. {_project_label(project)}")

    while True:
        answer = input("Selecciona un proyecto [1]: ").strip() or "1"
        try:
            index = int(answer)
            if index < 1 or index > len(projects):
                raise ValueError
            return projects[index - 1]
        except ValueError:
            print("Selección inválida. Introduce el número mostrado en la lista.")


def build_group_path(project: Project, requested_path: Optional[str]) -> str:
    """Construye una ruta de grupo dentro del proyecto seleccionado."""
    project_path = str(project.get("normalizedName") or "").strip().rstrip("/\\")
    if not project_path:
        raise ValueError("El proyecto no contiene el campo normalizedName")

    path = (requested_path or "").strip().replace("\\", "/")
    if not path:
        return project_path
    if path.startswith("/"):
        if path != project_path and not path.startswith(project_path + "/"):
            raise ValueError(
                f"La ruta {path!r} no pertenece al proyecto {project_path!r}"
            )
        return path.rstrip("/")
    return f"{project_path}/{path.strip('/')}"


def sync_project(
    group_path: str,
    download_dir: Path,
    *,
    rocket_url: Optional[str] = None,
    api_token: Optional[str] = None,
    verify_ssl: bool = True,
    force: bool = False,
) -> None:
    """Ejecuta el flujo de sincronización oficial de py2rocket."""
    command = [
        sys.executable,
        "-m",
        "py2rocket",
        "sync",
        group_path,
        "--output",
        str(download_dir),
    ]
    if rocket_url:
        command.extend(["--url", rocket_url])
    if api_token:
        command.extend(["--token", api_token])
    if not verify_ssl:
        command.append("--no-verify-ssl")
    if force:
        command.append("--force")

    subprocess.run(command, check=True)


def find_workflows(download_dir: Path) -> Iterable[Path]:
    """Encuentra los DSL descargados, ignorando archivos auxiliares."""
    return sorted(
        path
        for path in download_dir.rglob("*.py")
        if path.is_file() and not path.name.endswith("_databricks.py")
    )


def convert_workflows(
    download_dir: Path,
    notebooks_dir: Path,
    unity_catalog_mapping_file: Optional[str] = None,
    template_replacement: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Path], List[Tuple[Path, str]]]:
    """Convierte los DSL y conserva su ruta relativa en la salida."""
    converted: List[Path] = []
    errors: List[Tuple[Path, str]] = []

    for workflow_file in find_workflows(download_dir):
        relative_path = workflow_file.relative_to(download_dir)
        notebook_file = notebooks_dir / relative_path
        notebook_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            build_databricks(
                workflow_file=str(workflow_file),
                output_path=str(notebook_file),
                unity_catalog_mapping_file=unity_catalog_mapping_file,
                template_replacement=template_replacement,
            )
            converted.append(notebook_file)
        except Exception as exc:  # Continúa migrando los demás assets.
            errors.append((workflow_file, str(exc)))
            print(f"[!] No se pudo convertir {workflow_file}: {exc}")

    return converted, errors


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Descarga assets de Rocket y los convierte a notebooks Databricks"
    )
    parser.add_argument(
        "--project", help="ID, nombre o normalizedName (si se omite, pregunta)"
    )
    parser.add_argument(
        "--group-path",
        help="Ruta absoluta o relativa del grupo (default: proyecto completo)",
    )
    parser.add_argument(
        "-o", "--output", help="Ruta local de migración (si se omite, pregunta)"
    )
    parser.add_argument("--url", help="URL de Rocket (o ROCKET_API_HOST)")
    parser.add_argument("--token", help="Cookie de Rocket (o ROCKET_AUTH_COOKIE)")
    parser.add_argument(
        "--unity-catalog-map", help="Mapping JSON opcional de Unity Catalog"
    )
    parser.add_argument("--force", action="store_true", help="Sobrescribe descargas")
    parser.add_argument(
        "--no-verify-ssl", action="store_true", help="Desactiva la validación SSL"
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    load_dotenv()
    args = _parse_args(argv)
    verify_ssl = not args.no_verify_ssl
    if not args.no_verify_ssl:
        verify_ssl = os.getenv("ROCKET_VERIFY_SSL", "true").lower() == "true"

    result = get_projects(
        rocket_url=args.url,
        api_token=args.token,
        verify_ssl=verify_ssl,
    )
    if result.get("status") != "success":
        print(f"Error consultando proyectos: {result.get('message')}")
        return 1

    try:
        requested_project = args.project or os.getenv("PY2ROCKET_MIGRATION_PROJECT")
        project = select_project(result.get("projects", []), requested_project)

        requested_group = args.group_path
        if requested_group is None:
            requested_group = os.getenv("PY2ROCKET_MIGRATION_GROUP_PATH")
        if requested_group is None:
            default_group = project.get("normalizedName") or ""
            requested_group = input(
                f"Ruta del grupo en Rocket [{default_group}]: "
            ).strip()
        group_path = build_group_path(project, requested_group)

        output_value = args.output or os.getenv("PY2ROCKET_MIGRATION_OUTPUT")
        if output_value is None:
            output_value = input("Ruta local de salida [migracion_databricks]: ").strip()
        migration_dir = Path(output_value or "migracion_databricks").expanduser()
        download_dir = migration_dir / "rocket"
        notebooks_dir = migration_dir / "databricks"

        print(f"\n[*] Descargando {group_path} en {download_dir}...")
        sync_project(
            group_path,
            download_dir,
            rocket_url=args.url,
            api_token=args.token,
            verify_ssl=verify_ssl,
            force=args.force or _env_bool("PY2ROCKET_MIGRATION_FORCE"),
        )

        print(f"\n[*] Convirtiendo workflows en {notebooks_dir}...")
        mapping_file = args.unity_catalog_map or os.getenv(
            "PY2ROCKET_UNITY_CATALOG_MAP"
        )
        converted, errors = convert_workflows(
            download_dir,
            notebooks_dir,
            mapping_file or None,
            template_replacement_from_env(),
        )
    except (ValueError, OSError, subprocess.CalledProcessError) as exc:
        print(f"Error durante la migración: {exc}")
        return 1

    print("\nResumen de migración:")
    print(f"  Proyecto: {_project_label(project)}")
    print(f"  Grupo: {group_path}")
    print(f"  Notebooks generados: {len(converted)}")
    print(f"  Errores de conversión: {len(errors)}")
    print(f"  Salida: {notebooks_dir.resolve()}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
