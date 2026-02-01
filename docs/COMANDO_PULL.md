# Comando pull - Documentación

## Descripción

El comando `pull` descarga un workflow desde el servidor Stratio Rocket usando el endpoint `/workflows/download/{id}`.

## Características

✅ **Múltiples formatos de entrada**: Acepta archivos `.py`, `.json` o sin extensión
✅ **Detección automática del workflow_id**:

- Para `.json`: Lee el campo `id` del archivo
- Para `.py`: Busca el `.json` compilado y lee el campo `id`
  ✅ **Gestión de sobrescritura**: Pregunta al usuario si el archivo ya existe
  ✅ **Opciones de guardado**:
- Reemplazar archivo existente
- Guardar con sufijo `_server` (ej: `workflow_server.json`)
- Cancelar operación
  ✅ **Modo fuerza**: Opción `--force` para sobrescribir sin preguntar

## Uso

### Línea de comandos

```bash
# Usando variables de entorno (.env)
py2rocket pull test_arity.json
py2rocket pull test_arity.py
py2rocket pull test_arity

# Con parámetros explícitos
py2rocket pull test_arity.json --url https://rocket.example.com --token cookie_auth

# Forzar sobrescritura
py2rocket pull test_arity.json --force

# Especificar archivo de salida
py2rocket pull test_arity.json --output backup.json

# Sin verificar SSL
py2rocket pull test_arity.json --no-verify-ssl
```

### API Python

```python
from py2rocket import pull

# Descargar workflow
result = pull(
    workflow_file="test_arity.json",
    rocket_url="https://rocket.example.com",
    api_token="cookie_auth",
    output_file=None,  # Opcional
    force_overwrite=False,  # Opcional
    verify_ssl=True,  # Opcional
)

if result["status"] == "success":
    print(f"Descargado: {result['output_file']}")
```

## Endpoint utilizado

```
GET /workflows/download/{workflow_id}
```

**Headers:**

- `Accept: application/json, text/plain, */*`
- `User-Agent: py2rocket/{version}`

**Cookies:**

- `stratio-cookie: {api_token}`
- `lang: en`

## Flujo de trabajo típico

```bash
# 1. Desplegar un workflow
py2rocket push mi_workflow.json

# 2. Hacer cambios en Rocket UI...

# 3. Descargar la versión actualizada del servidor
py2rocket pull mi_workflow.py
# → Pregunta: ¿Reemplazar o guardar como mi_workflow_server.json?

# 4. Revisar diferencias
# Comparar mi_workflow.json vs mi_workflow_server.json

# 5. Decidir qué versión usar
```

## Lógica de obtención del workflow_id

1. **Si el archivo es `.json`**:
   - Lee el archivo JSON
   - Obtiene el campo `id`
2. **Si el archivo es `.py` o sin extensión**:
   - Busca el archivo `.json` con el mismo nombre base
   - Lee el archivo JSON
   - Obtiene el campo `id`

3. **Si no se encuentra el workflow_id**:
   - Lanza un error indicando que no se pudo obtener el ID

## Lógica de guardado

1. **Si el archivo NO existe**:
   - Guarda directamente con el nombre especificado

2. **Si el archivo SÍ existe y `--force` está activo**:
   - Sobrescribe sin preguntar

3. **Si el archivo SÍ existe y `--force` NO está activo**:
   - Pregunta al usuario:
     - Opción 1: Reemplazar
     - Opción 2: Guardar como `{nombre}_server.json`
     - Opción 3: Cancelar

## Variables de entorno

El comando respeta las siguientes variables de entorno (archivo `.env`):

- `ROCKET_API_HOST`: URL del servidor Rocket
- `ROCKET_AUTH_COOKIE`: Cookie de autenticación
- `ROCKET_VERIFY_SSL`: Verificar certificados SSL (default: True)

## Errores comunes

### Error: "No se pudo obtener el workflow_id"

**Causa**: El archivo JSON no existe o no contiene el campo `id`

**Solución**:

```bash
# Verificar que el JSON existe y fue compilado
py2rocket build mi_workflow.py
# Luego intentar pull
py2rocket pull mi_workflow.py
```

### Error: "Debe proporcionar 'rocket_url'"

**Causa**: No se configuró ROCKET_API_HOST en .env ni se pasó --url

**Solución**:

```bash
# Opción 1: Configurar .env
echo "ROCKET_API_HOST=https://rocket.example.com" >> .env

# Opción 2: Pasar parámetro
py2rocket pull workflow.json --url https://rocket.example.com
```

### Error: "Error al descargar el workflow: 404"

**Causa**: El workflow_id no existe en el servidor

**Solución**: Verificar que el workflow fue desplegado primero con `push`

## Integración con otros comandos

```bash
# Ciclo completo de desarrollo
py2rocket create mi_workflow              # Crear
py2rocket build mi_workflow.py            # Compilar
py2rocket push mi_workflow.json           # Desplegar
py2rocket run mi_workflow.json            # Ejecutar
py2rocket pull mi_workflow.py             # Descargar versión del servidor
```
