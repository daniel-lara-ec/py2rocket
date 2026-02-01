# Comando download - Implementación Completada

## Resumen

Se ha implementado exitosamente el comando `download` que descarga workflows desde Rocket usando su ID directo.

## Características Principales

### 1. **Descargar por ID**

- Toma un workflow ID (UUID) como parámetro
- Realiza descarga directa del servidor sin necesidad de archivo local

### 2. **Nombre de archivo automático**

- Usa el campo `name` del workflow descargado como nombre de archivo
- Sanitiza caracteres especiales (espacios, slashes, etc.) por underscores
- Ejemplo: `"my workflow"` → `my_workflow.json`

### 3. **Gestión de conflictos**

Si el archivo ya existe, ofrece 3 opciones:

1. **Reemplazar**: Sobrescribe el archivo existente
2. **Guardar como \_server**: Agrega `_server` antes de la extensión
   - Ejemplo: `workflow.json` → `workflow_server.json`
3. **Cancelar**: Cancela la operación

### 4. **Configuración**

Soporta los siguientes parámetros:

- `--url`: URL de Rocket (o variable de entorno ROCKET_URL)
- `--token`: Cookie de autenticación (o ROCKET_AUTH_COOKIE)
- `--force`: Fuerza sobrescritura sin preguntar
- `--no-verify-ssl`: Desactiva verificación SSL (desarrollo)

## Uso

### Línea de comandos

```bash
# Descarga básica (usa variables de entorno)
python -m py2rocket download 7133a9b4-d4fc-4390-9aa1-802d836a2874

# Con parámetros explícitos
python -m py2rocket download 7133a9b4-d4fc-4390-9aa1-802d836a2874 \
    --url https://rocket.example.com \
    --token mi-token

# Forzar sobrescritura
python -m py2rocket download 7133a9b4-d4fc-4390-9aa1-802d836a2874 --force
```

### Uso programático

```python
from py2rocket import download

result = download(
    workflow_id="7133a9b4-d4fc-4390-9aa1-802d836a2874",
    rocket_url="https://rocket.example.com",
    api_token="my-token"
)

print(result)
# {
#     'status': 'success',
#     'message': 'Workflow descargado exitosamente',
#     'workflow_id': '7133a9b4-d4fc-4390-9aa1-802d836a2874',
#     'workflow_name': 'my-workflow',
#     'output_file': 'my_workflow.json',
#     'url': 'https://rocket.example.com/workflows/download/...'
# }
```

## Diferencia entre pull y download

| Aspecto            | pull                            | download                         |
| ------------------ | ------------------------------- | -------------------------------- |
| **Entrada**        | Archivo local (.py, .json)      | ID del workflow (UUID)           |
| **Workflow ID**    | Se extrae del archivo local     | Se proporciona directamente      |
| **Nombre archivo** | Usa el nombre del archivo local | Usa el campo `name` del workflow |
| **Uso típico**     | Sincronizar cambios locales     | Descargar por primera vez        |

## Cambios realizados

### 1. **py2rocket/**init**.py**

- ✅ Agregada función `download()` (líneas ~735-834)
- ✅ Actualizado `__all__` para incluir "download"
- ✅ Implementa misma lógica de autenticación y manejo de errores que `pull()`

### 2. **py2rocket/cli.py**

- ✅ Agregado import de `download` (línea 20)
- ✅ Agregada función `cmd_download()` (líneas ~430-502)
- ✅ Agregado parser del comando `download` (líneas ~671-688)
- ✅ Actualizado docstring del módulo

### 3. **README.md**

- ✅ Agregada documentación del comando `download`
- ✅ Comparación entre `pull` y `download`
- ✅ Ejemplos de uso

### 4. **Archivos de prueba**

- ✅ `test_download_function.py`: Verifica lógica de generación de nombres
- ✅ `EJEMPLOS_DOWNLOAD.md`: Ejemplos de uso del comando

## Validación

✅ Módulos compilados sin errores
✅ Comando aparece en el help de py2rocket
✅ Función es importable desde py2rocket
✅ Lógica de generación de nombres funciona correctamente
✅ Manejo de conflictos (archivo existente) implementado
✅ Integración con sistema de autenticación existente

## Próximos pasos (opcional)

- Agregar alias cortos para parámetros comunes
- Implementar caché de workflows descargados recientemente
- Agregar opción para listar workflows antes de descargar
