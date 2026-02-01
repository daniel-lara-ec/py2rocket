# Comando download - Ejemplos de uso

## Uso básico

# Descarga un workflow por su ID (usando variables de entorno para URL y token)

python -m py2rocket download 7133a9b4-d4fc-4390-9aa1-802d836a2874

## Con parámetros explícitos

python -m py2rocket download 7133a9b4-d4fc-4390-9aa1-802d836a2874 \
 --url https://rocket.example.com \
 --token mi-token-secreto

## Forzar sobrescritura sin preguntar

python -m py2rocket download 7133a9b4-d4fc-4390-9aa1-802d836a2874 --force

## Sin verificar SSL (para entornos de desarrollo)

python -m py2rocket download 7133a9b4-d4fc-4390-9aa1-802d836a2874 --no-verify-ssl

## Comportamiento del comando:

# 1. Descarga el workflow usando el ID proporcionado

# 2. Usa el campo 'name' del workflow como nombre de archivo

# 3. Si el archivo existe:

# - Opción 1: Reemplazar el archivo existente

# - Opción 2: Guardar con sufijo \_server (ej: workflow_server.json)

# - Opción 3: Cancelar la operación

## Diferencia entre pull y download:

# - pull: usa un archivo local para extraer el workflow_id y descarga

# - download: descarga directamente usando el workflow_id proporcionado

# y el nombre del archivo se toma del campo 'name' del workflow
