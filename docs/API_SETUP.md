# Configuración de la API de Rocket

## Configuración Inicial

1. Copia el archivo `.env.example` a `.env`:

   ```bash
   cp .env.example .env
   ```

2. Edita el archivo `.env` con tus credenciales:
   ```
   ROCKET_API_HOST=https://your-rocket-host.com
   ROCKET_API_TOKEN=your_api_token_here
   ```

## Uso del Comando Create con API

El comando `create` ahora puede conectarse automáticamente a la API de Rocket para obtener los UUIDs de proyecto y grupo.

### Sintaxis

```bash
python -m py2rocket create <nombre> \
  --description "Descripción del pipeline" \
  --project-name "nombre-del-proyecto" \
  --group-name "/ruta/del/grupo/assets/"
```

### Ejemplo Completo

```bash
python -m py2rocket create pl-transformacion-prueba \
  --description "Asset para probar la ejecución" \
  --project-name "cda-sandbox" \
  --group-name "/home/cda-sandbox/dalarana/pruebas/assets/"
```

### Proceso Automático

Cuando usas `--project-name` y `--group-name`, la herramienta:

1. 🔍 Se conecta a `{ROCKET_API_HOST}/projects/findByName/{projectName}`
2. ✓ Obtiene el `projectId` (UUID)
3. 🔍 Se conecta a `{ROCKET_API_HOST}/groups/findByName?name={groupName}`
4. ✓ Obtiene el `groupId` (UUID)
5. 📝 Genera el archivo del pipeline con los IDs configurados

### Headers de Navegador

La herramienta simula automáticamente un navegador Edge en las peticiones API con headers completos:

- User-Agent de Edge
- Headers de seguridad (Sec-Fetch-\*)
- Headers de Chrome/Edge (sec-ch-ua)
- Accept-Language y Accept-Encoding

## Comandos Disponibles

### Crear pipeline sin API (básico)

```bash
python -m py2rocket create mi-pipeline --description "Mi pipeline"
```

### Crear pipeline con API (verificación automática)

```bash
python -m py2rocket create mi-pipeline \
  --description "Mi pipeline" \
  --project-name "mi-proyecto" \
  --group-name "/ruta/grupo"
```

### Crear pipeline en modo offline (sin verificación)

```bash
python -m py2rocket create mi-pipeline \
  --description "Mi pipeline" \
  --project-name "mi-proyecto" \
  --group-name "/ruta/grupo" \
  --offline
```

⚠️ **Modo Offline**: Omite la verificación de API. El pipeline debe ser configurado y subido manualmente en Rocket.

### Compilar a JSON

```bash
python -m py2rocket build mi-pipeline.py
```

## Modo Offline

El flag `--offline` permite crear pipelines sin conectarse a la API de Rocket. Esto es útil cuando:

- No tienes acceso a la API en el momento
- Estás trabajando en un entorno sin conectividad
- Quieres crear múltiples pipelines rápidamente

**⚠️ Advertencia**: Cuando usas `--offline`:

- Los IDs de proyecto y grupo NO son validados
- Debes configurar manualmente el pipeline en Rocket
- El pipeline debe ser subido a través de la interfaz web de Rocket
- Verifica que el proyecto y grupo existan antes de compilar

## Solución de Problemas

### Error: Variables de entorno no configuradas

```
❌ Error: Para usar --project-name y --group-name, debes configurar:
   ROCKET_API_HOST y ROCKET_API_TOKEN en el archivo .env
```

**Solución**: Crea y configura el archivo `.env` con tus credenciales.

### Error: Proyecto no encontrado

```
❌ Error: No se encontró el ID del proyecto 'nombre-proyecto'
```

**Solución**: Verifica que el nombre del proyecto sea correcto y exista en Rocket.

### Error: Grupo no encontrado

```
❌ Error: No se encontró el ID del grupo '/ruta/grupo'
```

**Solución**: Verifica que la ruta del grupo sea correcta y exista en Rocket.
