# Archivo .py2rocket

## Descripción

El archivo `.py2rocket` es un archivo de metadatos en formato JSON que se crea automáticamente cuando se ejecuta el comando `py2rocket sync`. Este archivo permite a las extensiones y herramientas identificar que una carpeta fue creada mediante una sincronización con un proyecto de Rocket.

## Ubicación

El archivo `.py2rocket` se crea en la raíz de la carpeta de salida especificada durante la sincronización.

## Formato

El archivo contiene la siguiente estructura JSON:

```json
{
  "sync_info": {
    "project_name": "Nombre del Proyecto",
    "project_code": "codigo_proyecto",
    "group_name": "Grupo/Base/Path",
    "group_id": "uuid-del-grupo",
    "sync_date": "2026-02-12T10:30:45.123456"
  }
}
```

## Campos

### sync_info

Objeto que contiene toda la información relacionada con la sincronización.

- **project_name** (string): Nombre del proyecto asociado al grupo sincronizado. Puede estar vacío si el grupo no tiene un proyecto asociado.

- **project_code** (string): Código o ID del proyecto. Puede estar vacío si el grupo no tiene un proyecto asociado.

- **group_name** (string): Nombre completo del grupo base que se sincronizó (incluye la ruta completa del grupo).

- **group_id** (string): UUID del grupo que se sincronizó.

- **sync_date** (string): Fecha y hora en formato ISO 8601 de cuándo se realizó la sincronización.

## Ejemplo de uso

### Comando de sincronización

```bash
py2rocket sync "MiProyecto/GrupoBase" --output ./mi_proyecto
```

### Archivo .py2rocket generado

```json
{
  "sync_info": {
    "project_name": "MiProyecto",
    "project_code": "mi_proyecto_001",
    "group_name": "MiProyecto/GrupoBase",
    "group_id": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
    "sync_date": "2026-02-12T14:25:30.123456"
  }
}
```

## Uso en extensiones

Las extensiones de VS Code u otras herramientas pueden leer este archivo para:

1. **Identificar proyectos sincronizados**: Verificar si una carpeta fue creada mediante sincronización.
2. **Reconectar con Rocket**: Usar el `group_id` para volver a sincronizar o actualizar.
3. **Mostrar información contextual**: Mostrar el nombre del proyecto y grupo en la UI.
4. **Tracking de cambios**: Comparar la fecha de sincronización para determinar si hay cambios pendientes.

### Ejemplo de lectura en Python

```python
import json
from pathlib import Path

def read_py2rocket_metadata(directory: Path) -> dict:
    """Lee metadatos del archivo .py2rocket si existe."""
    metadata_file = directory / ".py2rocket"
    if metadata_file.exists():
        return json.loads(metadata_file.read_text(encoding="utf-8"))
    return None

# Uso
metadata = read_py2rocket_metadata(Path("./mi_proyecto"))
if metadata:
    sync_info = metadata.get("sync_info", {})
    print(f"Proyecto: {sync_info.get('project_name')}")
    print(f"Grupo: {sync_info.get('group_name')}")
    print(f"Última sincronización: {sync_info.get('sync_date')}")
```

### Ejemplo de lectura en TypeScript (VS Code Extension)

```typescript
import * as fs from "fs";
import * as path from "path";

interface SyncInfo {
  project_name: string;
  project_code: string;
  group_name: string;
  group_id: string;
  sync_date: string;
}

interface Py2RocketMetadata {
  sync_info: SyncInfo;
}

function readPy2RocketMetadata(directory: string): Py2RocketMetadata | null {
  const metadataPath = path.join(directory, ".py2rocket");

  if (fs.existsSync(metadataPath)) {
    const content = fs.readFileSync(metadataPath, "utf-8");
    return JSON.parse(content);
  }

  return null;
}

// Uso
const metadata = readPy2RocketMetadata("./mi_proyecto");
if (metadata) {
  const { sync_info } = metadata;
  console.log(`Proyecto: ${sync_info.project_name}`);
  console.log(`Grupo: ${sync_info.group_name}`);
  console.log(`Última sincronización: ${sync_info.sync_date}`);
}
```

## Notas

- El archivo `.py2rocket` se sobrescribe cada vez que se ejecuta el comando `sync` en la misma carpeta.
- Si el comando `sync` se ejecuta con `--force`, el archivo se actualizará con la nueva fecha de sincronización.
- El archivo está en formato JSON para facilitar su lectura por diferentes lenguajes y herramientas.
- Se recomienda agregar `.py2rocket` a `.gitignore` si no se desea versionar esta información.
