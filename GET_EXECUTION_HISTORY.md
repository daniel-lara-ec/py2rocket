# Comando get-history - Obtener Historial de Ejecuciones

## Descripción

El comando `py2rocket get-history` permite obtener el historial de ejecuciones de un workflow desde Stratio Rocket y devolver los resultados en formato JSON.

## Sintaxis

```bash
py2rocket get-history <workflow-id> [opciones]
```

## Parámetros Obligatorios

| Parámetro     | Descripción                                                    |
| ------------- | -------------------------------------------------------------- |
| `workflow_id` | ID único del workflow (UUID) para el cual obtener el historial |

## Opciones

| Opción                    | Descripción                                                                                                         |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `--project-id PROJECT_ID` | ID del proyecto en Rocket                                                                                           |
| `--url URL`               | URL base de Rocket (ej: https://rocket.example.com). Por defecto usa `ROCKET_API_HOST` del .env                     |
| `--token TOKEN`           | Cookie de autenticación. Por defecto usa `ROCKET_AUTH_COOKIE` del .env                                              |
| `--status STATUS`         | Filtrar por estado (ej: `Running`, `Completed`, `Failed`, `Stopped`). Soporta múltiples valores separados por comas |
| `--limit LIMIT`           | Número máximo de ejecuciones a obtener (default: 50)                                                                |
| `--offset OFFSET`         | Número de resultados a saltar para paginación (default: 0)                                                          |
| `-o, --output OUTPUT`     | Ruta del archivo JSON donde guardar el historial                                                                    |
| `-j, --json-output`       | Mostrar salida en formato JSON en la consola                                                                        |
| `--no-verify-ssl`         | No verificar certificados SSL                                                                                       |
| `-h, --help`              | Mostrar ayuda                                                                                                       |

## Ejemplos

### 1. Obtener historial básico

Muestra un resumen tabular de las últimas 50 ejecuciones:

```bash
py2rocket get-history 67d9dbbc-3d7b-4611-ba2f-aaefdb431a10
```

**Salida:**

```
✓ Historial de ejecuciones obtenido exitosamente
  Workflow ID: 67d9dbbc-3d7b-4611-ba2f-aaefdb431a10
  Total de ejecuciones: 12

----------------------------------------------------------------------------------------------------
Execution ID                                 State           Created
----------------------------------------------------------------------------------------------------
abc12345-6789-4def-8901-234567890abc        Completed       2026-02-16T10:30:45.123Z
def67890-1234-4ghi-5678-901234567890        Completed       2026-02-15T15:22:10.456Z
ghi01234-5678-4jkl-9012-345678901234        Failed          2026-02-14T08:15:32.789Z
...
```

### 2. Obtener historial en formato JSON

```bash
py2rocket get-history 67d9dbbc-3d7b-4611-ba2f-aaefdb431a10 -j
```

**Salida:**

```json
{
  "status": "success",
  "message": "Historial de ejecuciones obtenido exitosamente",
  "workflow_id": "67d9dbbc-3d7b-4611-ba2f-aaefdb431a10",
  "total_count": 12,
  "executions": [
    {
      "id": "abc12345-6789-4def-8901-234567890abc",
      "executionNameDescription": {
        "name": "Ejecución Manual #1",
        "description": "Prueba de carga"
      },
      "statuses": [
        {
          "state": "Completed",
          "lastUpdateDate": "2026-02-16T10:35:45.789Z"
        }
      ],
      "creationDate": "2026-02-16T10:30:45.123Z",
      "assetVersionId": "workflow-v1",
      "archived": false,
      ...
    },
    ...
  ]
}
```

### 3. Guardar historial en archivo JSON

```bash
py2rocket get-history 67d9dbbc-3d7b-4611-ba2f-aaefdb431a10 -o execution_history.json
```

### 4. Filtrar por estado específico

```bash
py2rocket get-history 67d9dbbc-3d7b-4611-ba2f-aaefdb431a10 --status Completed -j
```

O múltiples estados:

```bash
py2rocket get-history 67d9dbbc-3d7b-4611-ba2f-aaefdb431a10 --status "Completed,Failed" -j
```

### 5. Usar con paginación

```bash
# Obtener primeros 20 resultados
py2rocket get-history 67d9dbbc-3d7b-4611-ba2f-aaefdb431a10 --limit 20

# Obtener resultados 21-40 (saltar 20)
py2rocket get-history 67d9dbbc-3d7b-4611-ba2f-aaefdb431a10 --limit 20 --offset 20
```

### 6. Con configuración completa

```bash
py2rocket get-history 67d9dbbc-3d7b-4611-ba2f-aaefdb431a10 \
  --project-id my-project \
  --url https://rocket.example.com \
  --token "my-auth-cookie" \
  --status Completed \
  --limit 100 \
  -o execution_history.json \
  -j
```

## Uso en Python

También puedes usar la función `get_execution_history` directamente en Python:

```python
from py2rocket import get_execution_history
import json

# Obtener historial
result = get_execution_history(
    workflow_id="67d9dbbc-3d7b-4611-ba2f-aaefdb431a10",
    project_id="my-project",
    status="Completed",
    limit=50
)

# Verificar resultado
if result["status"] == "success":
    print(f"Total de ejecuciones: {result['total_count']}")

    # Procesar ejecuciones
    for execution in result["executions"]:
        exec_id = execution["id"]
        states = execution.get("statuses", [])
        final_state = states[-1]["state"] if states else "Unknown"
        created = execution.get("creationDate")
        print(f"Ejecución {exec_id}: {final_state} ({created})")

    # Guardar en JSON
    with open("history.json", "w") as f:
        json.dump(result, f, indent=2, default=str)
```

## Configuración via Variables de Entorno

Las opciones pueden configurarse también via variables de entorno (.env):

```bash
# .env
ROCKET_API_HOST=https://rocket.example.com
ROCKET_AUTH_COOKIE=my-auth-cookie
PROJECT_ID=my-project
```

Luego el comando se simplifica:

```bash
py2rocket get-history 67d9dbbc-3d7b-4611-ba2f-aaefdb431a10 -j -o history.json
```

## Estructura de Respuesta

La respuesta JSON tiene la siguiente estructura:

```json
{
  "status": "success|error",
  "message": "Mensaje descriptivo",
  "workflow_id": "UUID del workflow",
  "total_count": "número total de ejecuciones encontradas",
  "executions": [
    {
      "id": "UUID de la ejecución",
      "executionNameDescription": {
        "name": "Nombre de la ejecución",
        "description": "Descripción"
      },
      "statuses": [
        {
          "state": "Running|Completed|Failed|Stopped",
          "lastUpdateDate": "timestamp ISO",
          "statusInfo": "información adicional"
        }
      ],
      "creationDate": "timestamp ISO",
      "endDate": "timestamp ISO",
      "assetVersionId": "UUID",
      "archived": true|false,
      "assetDataExecution": {
        "workflow": {...},
        "executionContext": {...}
      }
    }
  ],
  "url": "URL del endpoint consultado"
}
```

## Estados Posibles de Ejecución

- `Running` - Ejecución en progreso
- `Completed` - Ejecución finalizada exitosamente
- `Failed` - Ejecución falló
- `Stopped` - Ejecución detenida manualmente
- `Paused` - Ejecución pausada
- `Queued` - En cola de ejecución

## Casos de Uso

### Auditoría y Compliance

```bash
# Obtener historial completo de ejecuciones completadas
py2rocket get-history <workflow-id> --status Completed --limit 1000 -o audit.json
```

### Análisis de Errores

```bash
# Obtener solo ejecuciones fallidas
py2rocket get-history <workflow-id> --status Failed -j
```

### Dashboard y Reporte

```bash
# Exportar historial para procesamiento posterior
py2rocket get-history <workflow-id> --limit 100 -o history.json
# Procesar con jq, pandas o cualquier herramienta JSON
cat history.json | jq '.executions | length'
```

### Monitoreo Continuo

```bash
# Script para verificar ejecuciones periódicamente
#!/bin/bash
while true; do
  py2rocket get-history <workflow-id> --status Running
  sleep 60
done
```

## Notas

- El historial se obtiene del servidor de Rocket, mostrando todas las ejecuciones registradas
- El parámetro `--limit` afecta solo a la paginación del resultado, no al total de ejecuciones existentes
- Las fechas se devuelven en formato ISO 8601 (UTC)
- Requiere autenticación válida con Rocket
- Se recomienda usar referencias de proyecto y workflow correctas para asegurar resultados precisos
