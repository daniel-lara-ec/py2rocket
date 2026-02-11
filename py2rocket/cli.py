"""
CLI para py2rocket - Herramienta de línea de comandos

Comandos disponibles:
    py2rocket create <nombre> [opciones]   - Crea un nuevo workflow
    py2rocket build <archivo.py>           - Compila workflow a JSON
    py2rocket push <archivo.json>          - Despliega a Rocket
    py2rocket run <archivo.json>           - Ejecuta un workflow en Rocket
    py2rocket pull <archivo>               - Descarga workflow desde Rocket
    py2rocket download <workflow-id>       - Descarga workflow por ID desde Rocket
    py2rocket sync <grupo>                  - Sincroniza assets/workflows de un grupo a local
    py2rocket from-json <archivo.json>     - Convierte JSON a código Python
    py2rocket get-extensions               - Lista extensiones por proyecto
"""

import argparse
import sys
import json
import os
from pathlib import Path
from typing import Optional
from urllib.parse import quote
from dotenv import load_dotenv
import requests
import traceback
from tqdm import tqdm
import urllib3

from py2rocket import create, build, push, run, pull, download, from_json, __version__

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


def _sanitize_path_part(value: str) -> str:
    """Sanitiza un segmento para uso en rutas locales."""
    if value is None:
        return ""
    sanitized = value.strip()
    for sep in filter(None, [os.sep, os.altsep]):
        sanitized = sanitized.replace(sep, "_")
    for ch in [":", "*", "?", '"', "<", ">", "|"]:
        sanitized = sanitized.replace(ch, "_")
    return sanitized


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
        if not verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        if not verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
        # Determinar el archivo JSON real (soporta .py, .json o sin extensión)
        from pathlib import Path

        json_path = Path(args.json_file)
        if json_path.suffix == ".py" or json_path.suffix == "":
            json_path = json_path.with_suffix(".json")

        print(f"[⚙️] Ejecutando workflow desde: {json_path.name}")

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


def cmd_pull(args):
    """Comando: pull - Descarga workflow desde Rocket"""
    try:
        verify_ssl = _get_verify_ssl_from_env()
        if args.no_verify_ssl:
            verify_ssl = False

        # Intentar descargar el workflow
        result = pull(
            workflow_file=args.workflow_file,
            rocket_url=args.url,
            api_token=args.token,
            output_file=args.output,
            force_overwrite=args.force,
            verify_ssl=verify_ssl,
        )

        # Si necesita confirmación (archivo existe)
        if result.get("status") == "confirm_needed":
            output_path = Path(result["output_path"])
            print(f"\n⚠️  {result['message']}")
            print("\n¿Qué deseas hacer?")
            print("  1. Reemplazar el archivo existente")
            print("  2. Guardar con otro nombre (añadir '_server')")
            print("  3. Cancelar")

            choice = input("\nSelecciona una opción (1/2/3): ").strip()

            if choice == "1":
                # Reemplazar
                output_path.write_text(
                    json.dumps(result["workflow_data"], ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                print(f"\n✓ Workflow descargado y reemplazado: {output_path}")
                print(f"  Workflow ID: {result['workflow_id']}")
            elif choice == "2":
                # Guardar con _server
                base_name = output_path.stem
                new_output = output_path.parent / f"{base_name}_server.json"
                new_output.write_text(
                    json.dumps(result["workflow_data"], ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                print(f"\n✓ Workflow descargado como: {new_output}")
                print(f"  Workflow ID: {result['workflow_id']}")
            else:
                print("\n❌ Operación cancelada")
                sys.exit(0)

        elif result.get("status") == "success":
            print(f"\n✓ {result['message']}")
            print(f"  Archivo: {result['output_file']}")
            print(f"  Workflow ID: {result['workflow_id']}")
        else:
            print(f"\n❌ Error al descargar workflow: {result.get('message')}")
            sys.exit(1)

    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def cmd_download(args):
    """Comando: download - Descarga workflow por ID desde Rocket"""
    try:
        verify_ssl = _get_verify_ssl_from_env()
        if args.no_verify_ssl:
            verify_ssl = False

        # Intentar descargar el workflow
        result = download(
            workflow_id=args.workflow_id,
            api_token=args.token,
            force_overwrite=args.force,
            verify_ssl=verify_ssl,
        )

        # Si necesita confirmación (archivo existe)
        if result.get("status") == "confirm_needed":
            output_path = Path(result["output_path"])
            print(f"\n⚠️  {result['message']}")
            print("\n¿Qué deseas hacer?")
            print("  1. Reemplazar el archivo existente")
            print("  2. Guardar con otro nombre (añadir '_server')")
            print("  3. Cancelar")

            choice = input("\nSelecciona una opción (1/2/3): ").strip()

            if choice == "1":
                # Reemplazar
                output_path.write_text(
                    json.dumps(result["workflow_data"], ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                print(f"\n✓ Workflow descargado y reemplazado: {output_path}")
                print(f"  Workflow ID: {result['workflow_id']}")
                print(f"  Workflow Name: {result['workflow_name']}")
            elif choice == "2":
                # Guardar con _server
                base_name = output_path.stem
                new_output = output_path.parent / f"{base_name}_server.json"
                new_output.write_text(
                    json.dumps(result["workflow_data"], ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                print(f"\n✓ Workflow descargado como: {new_output}")
                print(f"  Workflow ID: {result['workflow_id']}")
                print(f"  Workflow Name: {result['workflow_name']}")
            else:
                print("\n❌ Operación cancelada")
                sys.exit(0)

        elif result.get("status") == "success":
            print(f"\n✓ {result['message']}")
            print(f"  Archivo: {result['output_file']}")
            print(f"  Workflow ID: {result['workflow_id']}")
            print(f"  Workflow Name: {result['workflow_name']}")
        else:
            print(f"\n❌ Error al descargar workflow: {result.get('message')}")
            sys.exit(1)

    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def cmd_sync(args):
    """Comando: sync - Sincroniza assets/workflows de un grupo hacia una ruta local"""
    try:
        api_host = args.url or os.getenv("ROCKET_API_HOST")
        auth_cookie = args.token or os.getenv("ROCKET_AUTH_COOKIE")
        if not api_host or not auth_cookie:
            print(
                "❌ Error: Configura ROCKET_API_HOST y ROCKET_AUTH_COOKIE en .env o pásalos por argumentos."
            )
            sys.exit(1)

        verify_ssl = _get_verify_ssl_from_env()
        if args.no_verify_ssl:
            verify_ssl = False

        group_name = args.group_name
        if not group_name:
            print("❌ Error: Debes indicar el nombre del grupo a sincronizar.")
            sys.exit(1)

        output_base = Path(args.output or ".")

        headers = {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "py2rocket/" + __version__,
        }
        cookies = {"stratio-cookie": auth_cookie, "lang": "en"}

        def _get_group_by_name(name: str) -> Optional[dict]:
            group_url = f"{api_host.rstrip('/')}/groups/findByName"
            response = requests.get(
                group_url,
                params={"name": name},
                headers=headers,
                cookies=cookies,
                verify=verify_ssl,
                timeout=30,
            )
            response.raise_for_status()
            return response.json() or {}

        def _extract_groups(payload) -> list:
            groups = []
            if isinstance(payload, dict):
                if payload.get("name"):
                    groups.append(
                        {"name": payload.get("name"), "id": payload.get("id")}
                    )
                for key in (
                    "children",
                    "subGroups",
                    "subgroups",
                    "childGroups",
                    "groupChildren",
                    "groups",
                ):
                    if key in payload:
                        groups.extend(_extract_groups(payload[key]))
                for value in payload.values():
                    if isinstance(value, (list, dict)):
                        groups.extend(_extract_groups(value))
            elif isinstance(payload, list):
                for item in payload:
                    groups.extend(_extract_groups(item))
            return groups

        def _sanitize_parts(parts: list) -> list:
            return [p for p in (_sanitize_path_part(part) for part in parts) if p]

        root_parts = [p for p in group_name.strip("/\\").split("/") if p]
        root_base = (
            _sanitize_path_part(root_parts[-1])
            if root_parts
            else _sanitize_path_part(group_name)
        )

        def _sync_group(name: str, group_id: str) -> tuple:
            # 2) Buscar assets del grupo (solo workflows)
            assets_url = f"{api_host.rstrip('/')}/assets/findAllByGroupDto/{group_id}"
            response = requests.get(
                assets_url,
                params={"assetType": "Workflow"},
                headers=headers,
                cookies=cookies,
                verify=verify_ssl,
                timeout=30,
            )
            response.raise_for_status()
            assets = response.json() or []

            # 3) Crear jerarquía local del grupo desde el último segmento del root
            group_parts = [p for p in name.strip("/\\").split("/") if p]
            rel_parts = group_parts
            if root_parts and group_parts[: len(root_parts)] == root_parts:
                rel_parts = group_parts[len(root_parts) :]
            rel_parts = _sanitize_parts(rel_parts)
            if rel_parts:
                group_dir = output_base / root_base / Path(*rel_parts)
            else:
                group_dir = output_base / root_base
            group_dir.mkdir(parents=True, exist_ok=True)

            group_assets = 0
            group_versions = 0
            group_downloaded = 0
            group_skipped = 0

            for asset_dto in tqdm(
                assets,
                desc=f"Assets {name}",
                unit="asset",
            ):
                workflow_asset = asset_dto.get("workflowAsset")
                if not workflow_asset:
                    continue

                asset_id = workflow_asset.get("id")
                asset_name = workflow_asset.get("name") or asset_id
                if not asset_id:
                    continue

                safe_asset_name = _sanitize_path_part(asset_name) or asset_id
                asset_dir = group_dir / safe_asset_name
                asset_dir.mkdir(parents=True, exist_ok=True)

                # Archivo identificador del asset
                name_file = asset_dir / "name.txt"
                if not name_file.exists() or args.force:
                    name_file.write_text(str(asset_name), encoding="utf-8")

                group_assets += 1

                # 4) Buscar versiones del asset
                versions_url = (
                    f"{api_host.rstrip('/')}/assets/findAllVersions/{asset_id}"
                )
                v_response = requests.get(
                    versions_url,
                    headers=headers,
                    cookies=cookies,
                    verify=verify_ssl,
                    timeout=30,
                )
                v_response.raise_for_status()
                versions = v_response.json() or []

                for version_info in tqdm(
                    versions,
                    desc=f"Versiones {asset_name}",
                    unit="ver",
                    leave=False,
                ):
                    version_id = version_info.get("id")
                    version_num = version_info.get("version")
                    if not version_id:
                        continue

                    group_versions += 1

                    file_name = (
                        f"v{version_num}.py"
                        if version_num is not None
                        else f"{version_id}.py"
                    )
                    output_file = asset_dir / file_name

                    if output_file.exists() and not args.force:
                        group_skipped += 1
                        print(f"⚠️  Saltando existente: {output_file}")
                        continue

                    workflow_url = (
                        f"{api_host.rstrip('/')}/workflows/download/{version_id}"
                    )
                    w_response = requests.get(
                        workflow_url,
                        headers=headers,
                        cookies=cookies,
                        verify=verify_ssl,
                        timeout=30,
                    )
                    w_response.raise_for_status()

                    try:
                        workflow_data = w_response.json()
                    except ValueError as exc:
                        print(
                            f"⚠️  Respuesta inválida al descargar {asset_name} v{version_num}: {exc}"
                        )
                        continue

                    # Guardar temporalmente JSON y convertir a Python DSL
                    temp_json = asset_dir / f"{output_file.stem}.json.tmp"
                    temp_json.write_text(
                        json.dumps(workflow_data, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                    try:
                        from_json(
                            json_file=str(temp_json), output_file=str(output_file)
                        )
                    finally:
                        try:
                            temp_json.unlink(missing_ok=True)
                        except Exception:
                            pass

                    group_downloaded += 1
                    print(f"✓ Descargado: {output_file}")

            return group_assets, group_versions, group_downloaded, group_skipped

        # 1) Encontrar el grupo por nombre (ruta)
        group_data = _get_group_by_name(group_name)
        group_id = group_data.get("id")
        if not group_id:
            print(f"❌ No se encontró el ID del grupo '{group_name}'.")
            sys.exit(1)

        print(f"✓ Grupo encontrado: {group_id}")

        # 1.1) Buscar subgrupos (recursivo si la API lo permite)
        group_targets = [{"name": group_name, "id": group_id}]
        try:
            subgroups_url = f"{api_host.rstrip('/')}/groups/findSubGroupsByName"
            sg_response = requests.get(
                subgroups_url,
                params={"name": group_name, "onlyFirstLevelChildren": False},
                headers=headers,
                cookies=cookies,
                verify=verify_ssl,
                timeout=30,
            )
            sg_response.raise_for_status()
            subgroup_payload = sg_response.json()
            subgroups = _extract_groups(subgroup_payload)
            seen = {group_name}
            for sg in subgroups:
                sg_name = sg.get("name")
                if not sg_name or sg_name in seen:
                    continue
                seen.add(sg_name)
                group_targets.append({"name": sg_name, "id": sg.get("id")})
        except requests.exceptions.RequestException:
            # Si falla, continuar solo con el grupo principal
            pass

        total_assets = 0
        total_versions = 0
        total_downloaded = 0
        total_skipped = 0

        for target in group_targets:
            target_name = target.get("name")
            target_id = target.get("id")
            if not target_name:
                continue
            if not target_id:
                resolved = _get_group_by_name(target_name)
                target_id = resolved.get("id")
            if not target_id:
                print(f"⚠️  No se pudo resolver el ID del grupo '{target_name}'.")
                continue

            assets, versions, downloaded, skipped = _sync_group(target_name, target_id)
            total_assets += assets
            total_versions += versions
            total_downloaded += downloaded
            total_skipped += skipped

        print("\nResumen de sincronización:")
        print(f"  - Assets: {total_assets}")
        print(f"  - Versiones: {total_versions}")
        print(f"  - Descargadas: {total_downloaded}")
        print(f"  - Omitidas: {total_skipped}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Error al consultar Rocket: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def cmd_from_json(args):
    """Comando: from-json - Convierte JSON de Rocket a código Python"""
    try:
        result = from_json(
            json_file=args.json_file,
            output_file=args.output,
        )

        if result.get("status") == "success":
            print(f"\n✓ {result['message']}")
            print(f"  Input: {result['input_file']}")
            print(f"  Output: {result['output_file']}")
            print(f"  Nodos: {result['nodes_count']} total")
            print(f"    - Inputs: {result['inputs']}")
            print(f"    - Transformations: {result['transforms']}")
            print(f"    - Outputs: {result['outputs']}")
        else:
            print(f"\n❌ Error al convertir JSON")
            sys.exit(1)

    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        sys.exit(1)


def cmd_get_extensions(args):
    """Comando: get-extensions - Lista extensiones por proyecto"""
    try:
        api_host = args.url or os.getenv("ROCKET_API_HOST")
        auth_cookie = args.token or os.getenv("ROCKET_AUTH_COOKIE")
        if not api_host or not auth_cookie:
            print(
                "❌ Error: Configura ROCKET_API_HOST y ROCKET_AUTH_COOKIE en .env o pásalos por argumentos."
            )
            sys.exit(1)

        default_project_id = os.getenv("PROJECT_ID")
        if default_project_id is not None:
            default_project_id = default_project_id.strip() or None

        while True:
            suffix = f" [{default_project_id}]" if default_project_id else ""
            project_id = input(f"ID del proyecto{suffix}: ").strip()
            if not project_id and default_project_id:
                project_id = default_project_id
            if project_id:
                break
            print("[!] El ID del proyecto es obligatorio y no puede estar vacío.")

        verify_ssl = _get_verify_ssl_from_env()
        if args.no_verify_ssl:
            verify_ssl = False

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

        response = requests.get(
            f"{api_host}/extensions/findAllByProjectId/{project_id}",
            headers=headers,
            cookies=cookies,
            verify=verify_ssl,
            timeout=30,
        )
        response.raise_for_status()
        extensions = response.json()

        if not isinstance(extensions, list) or not extensions:
            print("⚠️  No se encontraron extensiones para el proyecto.")
            return

        rows = []
        for item in extensions:
            rows.append(
                {
                    "id": str(item.get("id", "")),
                    "name": str(item.get("name", "")),
                    "extensionType": str(item.get("extensionType", "")),
                    "customClasses": str(item.get("customClasses", "")),
                }
            )

        headers_cols = ["id", "name", "extensionType", "customClasses"]

        max_custom_classes_width = 80
        max_widths = {
            "id": 36,
            "name": 40,
            "extensionType": 20,
            "customClasses": max_custom_classes_width,
        }

        def _wrap_text(text: str, width: int) -> list:
            if width <= 0:
                return [text]
            words = text.split()
            if not words:
                return [""]
            lines = []
            current = words[0]
            for word in words[1:]:
                if len(current) + 1 + len(word) <= width:
                    current += f" {word}"
                else:
                    lines.append(current)
                    current = word
            lines.append(current)
            return lines

        def _truncate(text: str, width: int) -> str:
            if width <= 0:
                return text
            if len(text) <= width:
                return text
            return text[: max(0, width - 1)] + "…"

        col_widths = {}
        for key in headers_cols:
            longest = max(len(r[key]) for r in rows)
            col_widths[key] = max(len(key), min(longest, max_widths[key]))

        header_line = " | ".join(key.ljust(col_widths[key]) for key in headers_cols)
        separator = "-+-".join("-" * col_widths[key] for key in headers_cols)
        print(header_line)
        print(separator)

        for r in rows:
            custom_lines = _wrap_text(r["customClasses"], col_widths["customClasses"])
            lines_count = max(1, len(custom_lines))
            for i in range(lines_count):
                row_id = _truncate(r["id"], col_widths["id"]) if i == 0 else ""
                row_name = _truncate(r["name"], col_widths["name"]) if i == 0 else ""
                row_type = (
                    _truncate(r["extensionType"], col_widths["extensionType"])
                    if i == 0
                    else ""
                )
                row_custom = custom_lines[i] if i < len(custom_lines) else ""
                print(
                    " | ".join(
                        [
                            row_id.ljust(col_widths["id"]),
                            row_name.ljust(col_widths["name"]),
                            row_type.ljust(col_widths["extensionType"]),
                            row_custom.ljust(col_widths["customClasses"]),
                        ]
                    )
                )

    except requests.exceptions.RequestException as e:
        print(f"❌ Error al consultar extensiones: {e}")
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
    parser_run.add_argument(
        "json_file", help="Archivo del pipeline (.py, .json o sin extensión)"
    )
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

    # Comando: pull
    parser_pull = subparsers.add_parser(
        "pull", help="Descarga un workflow desde Rocket"
    )
    parser_pull.add_argument(
        "workflow_file", help="Archivo del pipeline (.py, .json o sin extensión)"
    )
    parser_pull.add_argument(
        "-o", "--output", help="Nombre del archivo de salida (opcional)"
    )
    parser_pull.add_argument(
        "--url", help="URL de Rocket (o usar ROCKET_API_HOST env var)"
    )
    parser_pull.add_argument(
        "--token", help="Cookie de autenticación (o usar ROCKET_AUTH_COOKIE env var)"
    )
    parser_pull.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Forzar sobrescritura sin preguntar",
    )
    parser_pull.add_argument(
        "--no-verify-ssl", action="store_true", help="No verificar SSL"
    )
    parser_pull.set_defaults(func=cmd_pull)

    # Comando: download
    parser_download = subparsers.add_parser(
        "download", help="Descarga un workflow por su ID desde Rocket"
    )
    parser_download.add_argument(
        "workflow_id", help="ID del workflow a descargar (UUID)"
    )
    parser_download.add_argument(
        "--token", help="Cookie de autenticación (o usar ROCKET_AUTH_COOKIE env var)"
    )
    parser_download.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Forzar sobrescritura sin preguntar",
    )
    parser_download.add_argument(
        "--no-verify-ssl", action="store_true", help="No verificar SSL"
    )
    parser_download.set_defaults(func=cmd_download)

    # Comando: sync
    parser_sync = subparsers.add_parser(
        "sync", help="Sincroniza assets/workflows de un grupo hacia local"
    )
    parser_sync.add_argument(
        "group_name",
        help="Nombre del grupo (ruta) a sincronizar, ejemplo: /mi/grupo",
    )
    parser_sync.add_argument(
        "-o",
        "--output",
        help="Directorio base de salida (default: carpeta actual)",
    )
    parser_sync.add_argument(
        "--url", help="URL de Rocket (o usar ROCKET_API_HOST env var)"
    )
    parser_sync.add_argument(
        "--token",
        help="Cookie de autenticación (o usar ROCKET_AUTH_COOKIE env var)",
    )
    parser_sync.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Forzar sobrescritura sin preguntar",
    )
    parser_sync.add_argument(
        "--no-verify-ssl", action="store_true", help="No verificar SSL"
    )
    parser_sync.set_defaults(func=cmd_sync)

    # Comando: from-json
    parser_from_json = subparsers.add_parser(
        "from-json", help="Convierte JSON de Rocket a código Python DSL"
    )
    parser_from_json.add_argument(
        "json_file", help="Archivo JSON del workflow de Rocket"
    )
    parser_from_json.add_argument(
        "-o",
        "--output",
        help="Archivo Python de salida (default: mismo nombre con .py)",
    )
    parser_from_json.set_defaults(func=cmd_from_json)

    # Comando: get-extensions
    parser_get_extensions = subparsers.add_parser(
        "get-extensions", help="Lista extensiones por proyecto"
    )
    parser_get_extensions.add_argument(
        "--url", help="URL de Rocket (o usar ROCKET_API_HOST env var)"
    )
    parser_get_extensions.add_argument(
        "--token", help="Cookie de autenticación (o usar ROCKET_AUTH_COOKIE env var)"
    )
    parser_get_extensions.add_argument(
        "--no-verify-ssl", action="store_true", help="No verificar SSL"
    )
    parser_get_extensions.set_defaults(func=cmd_get_extensions)

    # Parsear argumentos
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Ejecutar comando
    args.func(args)


if __name__ == "__main__":
    main()
