"""
CLI para py2rocket - Herramienta de línea de comandos

Comandos disponibles:
    py2rocket create <nombre> [opciones]   - Crea un nuevo workflow
    py2rocket build <archivo.py>           - Compila workflow a JSON
    py2rocket push <archivo.json>          - Despliega a Rocket
    py2rocket run <archivo.json>           - Ejecuta un workflow en Rocket
"""

import argparse
import sys
import json
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
import requests

from py2rocket import create, build, push, run, __version__

# Cargar variables de entorno
load_dotenv()


def _get_verify_ssl_from_env() -> bool:
    """Obtiene ROCKET_VERIFY_SSL desde .env (default: True)."""
    value = os.getenv("ROCKET_VERIFY_SSL")
    if value is None:
        return True
    value = value.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return True


def cmd_create(args):
    """Comando: create - Crea un nuevo workflow"""
    try:
        # Parsear parámetros si se proporcionaron
        params = {}
        if args.params:
            try:
                params = json.loads(args.params)
            except json.JSONDecodeError:
                print("Error: Los parámetros deben estar en formato JSON válido")
                sys.exit(1)

        # Obtener configuración de API desde .env
        api_host = os.getenv("ROCKET_API_HOST")
        auth_cookie = os.getenv("ROCKET_AUTH_COOKIE")

        def _prompt_required(label: str, default: Optional[str] = None) -> str:
            while True:
                suffix = f" [{default}]" if default else ""
                value = input(f"{label}{suffix}: ").strip()
                if not value and default is not None:
                    value = default
                if value:
                    return value
                print("[!] Este valor es obligatorio y no puede estar vacío.")

        def _prompt_optional(label: str, default: Optional[str] = None) -> str:
            suffix = f" [{default}]" if default else ""
            value = input(f"{label}{suffix}: ").strip()
            if not value and default is not None:
                return default
            return value

        # Preguntar modo online/offline
        default_mode = "offline" if args.offline else "online"
        while True:
            mode_input = (
                input(f"¿Modo de ejecución? (online/offline) [{default_mode}]: ")
                .strip()
                .lower()
            )
            if not mode_input:
                mode_input = default_mode
            if mode_input in {"online", "o", "on", "1"}:
                online = True
            elif mode_input in {"offline", "f", "off", "0"}:
                online = False
            else:
                print("[!] Opción inválida. Usa online/offline.")
                continue
            if online and (not api_host or not auth_cookie):
                print(
                    "[!] Para modo online, configura ROCKET_API_HOST y ROCKET_AUTH_COOKIE en .env"
                )
                continue
            break

        verify_ssl = _get_verify_ssl_from_env()
        if args.no_verify_ssl:
            verify_ssl = False

        # Solicitar parámetros interactivos
        name = _prompt_required("Nombre del pipeline", args.name)
        description = _prompt_optional("Descripción", args.description)

        # Usar PROJECT_NAME del .env como default
        default_project = args.project_name or os.getenv("PROJECT_NAME")
        project_name = _prompt_optional("Nombre del proyecto", default_project)

        group_name = _prompt_optional("Nombre del grupo", args.group_name)

        # Variables para IDs
        project_id = None
        group_id = None
        asset_id = None

        if not project_name and os.getenv("PROJECT_ID"):
            project_id = os.getenv("PROJECT_ID")
            print(f"[*] Usando PROJECT_ID del .env: {project_id}")

        if online:
            # Headers simulando navegador Edge
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
            }

            cookies = {"stratio-cookie": auth_cookie, "lang": "en"}

            # Verificar proyecto
            while project_name:
                print(f"[*] Buscando proyecto: {project_name}...")
                try:
                    response = requests.get(
                        f"{api_host}/projects/findByName/{project_name}",
                        headers=headers,
                        cookies=cookies,
                        verify=verify_ssl,
                        timeout=30,
                    )
                    response.raise_for_status()
                    project_data = response.json()
                    project_id = project_data.get("id")
                    if not project_id:
                        print(f"❌ No se encontró el ID del proyecto '{project_name}'.")
                        project_name = _prompt_optional("Nombre del proyecto")
                        continue
                    print(f"✓ Proyecto encontrado: {project_id}")
                    break
                except requests.exceptions.RequestException as e:
                    print(f"❌ Error al buscar proyecto: {e}")
                    project_name = _prompt_optional("Nombre del proyecto")

            # Verificar grupo
            while group_name:
                print(f"🔍 Buscando grupo: {group_name}...")
                try:
                    response = requests.get(
                        f"{api_host}/groups/findByName",
                        params={"name": group_name},
                        headers=headers,
                        cookies=cookies,
                        verify=verify_ssl,
                        timeout=30,
                    )
                    response.raise_for_status()
                    group_data = response.json()
                    group_id = group_data.get("id")
                    if not group_id:
                        print(f"❌ No se encontró el ID del grupo '{group_name}'.")
                        group_name = _prompt_optional("Nombre del grupo")
                        continue
                    print(f"✓ Grupo encontrado: {group_id}")
                    break
                except requests.exceptions.RequestException as e:
                    print(f"❌ Error al buscar grupo: {e}")
                    group_name = _prompt_optional("Nombre del grupo")

            # Crear asset en Rocket
            if project_id and group_id:
                print("🔍 Creando asset en Rocket...")
                payload = {
                    "workflowAsset": {
                        "name": name,
                        "description": description or "",
                        "groupId": group_id,
                        "projectId": project_id,
                        "executionEngine": args.engine,
                    }
                }
                method = os.getenv("ROCKET_ASSETS_METHOD", "POST").upper()
                try:
                    response = requests.request(
                        method,
                        f"{api_host}/assets",
                        headers=headers,
                        cookies=cookies,
                        json=payload,
                        verify=verify_ssl,
                        timeout=30,
                    )
                    response.raise_for_status()
                    asset_data = response.json()
                    asset_id = (asset_data.get("workflowAsset", {}) or {}).get("id")
                    if not asset_id:
                        print("❌ Error: No se encontró el ID del asset creado")
                    else:
                        print(f"✓ Asset creado: {asset_id}")

                        # Obtener workflow_id desde findAllVersions
                        workflow_id = None
                        print("[⚙️] Obteniendo workflow ID...")
                        try:
                            versions_response = requests.get(
                                f"{api_host}/assets/findAllVersions/{asset_id}",
                                headers=headers,
                                cookies=cookies,
                                verify=verify_ssl,
                                timeout=30,
                            )
                            versions_response.raise_for_status()
                            versions_list = versions_response.json()

                            # Buscar el diccionario con version == 0
                            for version_item in versions_list:
                                if version_item.get("version") == 0:
                                    workflow_id = version_item.get("id")
                                    break

                            if workflow_id:
                                print(f"✓ Workflow ID obtenido: {workflow_id}")
                            else:
                                print("⚠️  No se encontró workflow con version 0")
                        except requests.exceptions.RequestException as e:
                            print(f"⚠️  Error al obtener workflow ID: {e}")
                except requests.exceptions.RequestException as e:
                    print(f"❌ Error al crear el asset: {e}")
            else:
                print("⚠️  No se pudo crear el asset porque falta projectId o groupId.")
        else:
            print("\n⚠️  MODO OFFLINE ACTIVADO")
            print("   Los IDs de proyecto y grupo NO fueron validados.")
            print("   Deberás configurar manualmente el pipeline en Rocket:")
            if group_name:
                print(f"   - Ruta del grupo: {group_name}")
            if project_name:
                print(f"   - Proyecto: {project_name}")
            print(
                "   - El pipeline debe ser subido manualmente a través de la interfaz de Rocket."
            )
            print()

        output_path = create(
            name=name,
            output_path=args.output,
            execution_engine=args.engine,
            params=params,
            description=description,
            project_id=project_id,
            group_id=group_id,
            asset_id=asset_id,
            workflow_id=workflow_id if online and asset_id else None,
        )

        print(f"\n👉 Siguiente paso: edita {output_path} y define tu pipeline")

    except FileExistsError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


def cmd_build(args):
    """Comando: build - Compila workflow a JSON"""
    try:
        workflow_file = args.workflow_file
        if Path(workflow_file).suffix == "":
            workflow_file = f"{workflow_file}.py"
        output_path = build(
            workflow_file=workflow_file,
            output_path=args.output,
            indent=args.indent,
        )

        print(
            f"\n👉 Siguiente paso: revisa {output_path} o despliega con 'py2rocket push'"
        )

    except (FileNotFoundError, ValueError) as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        sys.exit(1)


def cmd_push(args):
    """Comando: push - Despliega pipeline a Rocket"""
    try:
        # Manejar diferentes formatos de archivo
        json_file = args.json_file
        file_path = Path(json_file)

        if file_path.suffix == "":
            # Sin extensión: agregar .json
            json_file = f"{json_file}.json"
        elif file_path.suffix == ".py":
            # Con .py: cambiar a .json
            json_file = file_path.with_suffix(".json")
        # Si ya tiene .json, usar como está

        verify_ssl = _get_verify_ssl_from_env()
        if args.no_verify_ssl:
            verify_ssl = False
        result = push(
            json_file=str(json_file),
            rocket_url=args.url,
            api_token=args.token,
            project_id=args.project_id,
            group_id=args.group_id,
            verify_ssl=verify_ssl,
            dry_run=args.dry_run,
        )

        if result["status"] == "success":
            print(f"\n✓ Pipeline desplegado exitosamente")
            print(f"  ID: {result['pipeline_id']}")
            print(f"  URL: {result['url']}")
        else:
            print(f"\n❌ Error al desplegar: {result['message']}")
            sys.exit(1)

    except NotImplementedError as e:
        print(f"⚠️  {e}")
        sys.exit(2)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def cmd_run(args):
    """Comando: run - Ejecuta workflow en Rocket"""
    try:
        verify_ssl = _get_verify_ssl_from_env()
        if args.no_verify_ssl:
            verify_ssl = False
        result = run(
            json_file=args.json_file,
            workflow_id=args.workflow_id,
            project_id=args.project_id,
            rocket_url=args.url,
            api_token=args.token,
            instance=args.instance,
            extra_params_file=args.extra_params,
            verify_ssl=verify_ssl,
        )

        if result.get("status") == "success":
            print("\n✓ Workflow ejecutado exitosamente")
            if result.get("response"):
                print(json.dumps(result["response"], ensure_ascii=False, indent=2))
        else:
            print("\n❌ Error al ejecutar workflow")
            sys.exit(1)

    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def main():
    """Punto de entrada principal del CLI"""
    parser = argparse.ArgumentParser(
        description="py2rocket - DSL para generar pipelines de Stratio Rocket",
        epilog=(
            "Ejemplo interactivo:\n"
            "  py2rocket create\n"
            "\n"
            "Ejemplo online con parámetros:\n"
            "  py2rocket create MiPipeline --project-name ProyectoA --group-name GrupoA\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--version", action="version", version=f"py2rocket {__version__}"
    )

    subparsers = parser.add_subparsers(dest="command", help="Comando a ejecutar")

    # Comando: create
    parser_create = subparsers.add_parser(
        "create", help="Crea un nuevo archivo de workflow"
    )
    parser_create.add_argument("name", nargs="?", help="Nombre del pipeline")
    parser_create.add_argument(
        "-o", "--output", help="Ruta del archivo de salida (default: {name}.py)"
    )
    parser_create.add_argument(
        "-e",
        "--engine",
        default="Hybrid",
        choices=["Batch", "Streaming", "Hybrid"],
        help="Motor de ejecución (default: Hybrid)",
    )
    parser_create.add_argument(
        "-p",
        "--params",
        help='Parámetros en formato JSON (ej: \'{"P_TABLA": "tabla1"}\')',
    )
    parser_create.add_argument("-d", "--description", default="", help="Descripción")
    parser_create.add_argument(
        "--project-name", help="Nombre del proyecto (se buscará el UUID vía API)"
    )
    parser_create.add_argument(
        "--group-name", help="Nombre del grupo (se buscará el UUID vía API)"
    )
    parser_create.add_argument(
        "--offline",
        action="store_true",
        help="Crear sin verificación de API (requiere configuración manual en Rocket)",
    )
    parser_create.add_argument(
        "--no-verify-ssl", action="store_true", help="No verificar SSL"
    )
    parser_create.set_defaults(func=cmd_create)

    # Comando: build
    parser_build = subparsers.add_parser(
        "build", help="Compila un workflow a JSON de Rocket"
    )
    parser_build.add_argument("workflow_file", help="Archivo .py del workflow")
    parser_build.add_argument("-o", "--output", help="Ruta del archivo JSON de salida")
    parser_build.add_argument(
        "-i", "--indent", type=int, default=2, help="Indentación del JSON (default: 2)"
    )
    parser_build.set_defaults(func=cmd_build)

    # Comando: push
    parser_push = subparsers.add_parser(
        "push", help="Despliega un pipeline a Rocket vía API"
    )
    parser_push.add_argument("json_file", help="Archivo JSON del pipeline")
    parser_push.add_argument(
        "--url", help="URL de Rocket (o usar ROCKET_API_HOST env var)"
    )
    parser_push.add_argument(
        "--token", help="Cookie de autenticación (o usar ROCKET_AUTH_COOKIE env var)"
    )
    parser_push.add_argument("--project-id", help="ID del proyecto en Rocket")
    parser_push.add_argument("--group-id", help="ID del grupo en Rocket")
    parser_push.add_argument(
        "--no-verify-ssl", action="store_true", help="No verificar SSL"
    )
    parser_push.add_argument(
        "--dry-run", action="store_true", help="Simular sin desplegar"
    )
    parser_push.set_defaults(func=cmd_push)

    # Comando: run
    parser_run = subparsers.add_parser("run", help="Ejecuta un workflow en Rocket")
    parser_run.add_argument("json_file", help="Archivo JSON del pipeline")
    parser_run.add_argument(
        "--workflow-id",
        help="ID del workflow en Rocket (si no se especifica, usa el id del JSON)",
    )
    parser_run.add_argument("--project-id", help="ID del proyecto en Rocket")
    parser_run.add_argument("--url", help="URL de Rocket")
    parser_run.add_argument(
        "--token", help="Cookie de autenticación (o usar ROCKET_AUTH_COOKIE env var)"
    )
    parser_run.add_argument(
        "--instance",
        default="XS",
        help="Instancia a añadir en paramsLists (default: XS)",
    )
    parser_run.add_argument(
        "--extra-params",
        help="Ruta a JSON con lista de extraParams",
    )
    parser_run.add_argument(
        "--no-verify-ssl", action="store_true", help="No verificar SSL"
    )
    parser_run.set_defaults(func=cmd_run)

    # Parsear argumentos
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Ejecutar comando
    args.func(args)


if __name__ == "__main__":
    main()
