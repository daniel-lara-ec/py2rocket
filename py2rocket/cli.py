"""
CLI para py2rocket - Herramienta de línea de comandos

Comandos disponibles:
    py2rocket create <nombre> [opciones]   - Crea un nuevo workflow
    py2rocket build <archivo.py>           - Compila workflow a JSON
    py2rocket push <archivo.json>          - Despliega a Rocket
"""

import argparse
import sys
import json
from pathlib import Path
from typing import Optional

from py2rocket import create, build, push, __version__


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
        
        output_path = create(
            name=args.name,
            output_path=args.output,
            execution_engine=args.engine,
            params=params,
            description=args.description,
        )
        
        print(f"\n👉 Siguiente paso: edita {output_path} y define tu pipeline")
        
    except FileExistsError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        sys.exit(1)


def cmd_build(args):
    """Comando: build - Compila workflow a JSON"""
    try:
        output_path = build(
            workflow_file=args.workflow_file,
            output_path=args.output,
            indent=args.indent,
        )
        
        print(f"\n👉 Siguiente paso: revisa {output_path} o despliega con 'py2rocket push'")
        
    except (FileNotFoundError, ValueError) as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        sys.exit(1)


def cmd_push(args):
    """Comando: push - Despliega pipeline a Rocket"""
    try:
        result = push(
            json_file=args.json_file,
            rocket_url=args.url,
            api_token=args.token,
            project_id=args.project_id,
            group_id=args.group_id,
            verify_ssl=not args.no_verify_ssl,
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


def main():
    """Punto de entrada principal del CLI"""
    parser = argparse.ArgumentParser(
        description="py2rocket - DSL para generar pipelines de Stratio Rocket",
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
    parser_create.add_argument("name", help="Nombre del pipeline")
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
    parser_create.set_defaults(func=cmd_create)
    
    # Comando: build
    parser_build = subparsers.add_parser(
        "build", help="Compila un workflow a JSON de Rocket"
    )
    parser_build.add_argument("workflow_file", help="Archivo .py del workflow")
    parser_build.add_argument(
        "-o", "--output", help="Ruta del archivo JSON de salida"
    )
    parser_build.add_argument(
        "-i", "--indent", type=int, default=2, help="Indentación del JSON (default: 2)"
    )
    parser_build.set_defaults(func=cmd_build)
    
    # Comando: push
    parser_push = subparsers.add_parser(
        "push", help="Despliega un pipeline a Rocket vía API"
    )
    parser_push.add_argument("json_file", help="Archivo JSON del pipeline")
    parser_push.add_argument("--url", required=True, help="URL de Rocket")
    parser_push.add_argument(
        "--token", help="Token de API (o usar ROCKET_API_TOKEN env var)"
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
    
    # Parsear argumentos
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Ejecutar comando
    args.func(args)


if __name__ == "__main__":
    main()
