# py2rocket - DSL para Stratio Rocket

🚀 **Módulo Python para generar pipelines de Stratio Rocket de forma declarativa**

---

## 📦 Estructura del Proyecto

```
DSL/
├── py2rocket/                  # Módulo principal
│   ├── __init__.py            # API pública: create, build, push
│   ├── cli.py                 # CLI de comandos
│   ├── core/                  # Componentes core
│   │   ├── pipeline.py        # Pipeline, Node, Edge
│   │   ├── operations.py      # sql, pyspark, print_step
│   │   ├── decorators.py      # @pipeline decorator
│   │   └── compiler.py        # RocketCompiler
│   ├── templates/             # Plantillas
│   │   └── workflow_template.py
│   └── README.md              # Documentación del módulo
│
├── dsl_plantilla.py           # Ejemplo de workflow objetivo
├── ejemplo_uso.py             # Ejemplo funcional completo
├── resumen_dsl_rocket.md      # Documento de diseño
├── reusltado_workflow.json    # JSON esperado
├── pyproject.toml             # Configuración del paquete
└── README.md                  # Este archivo
```

---

## 🚀 Instalación

```bash
# Instalar en modo desarrollo
pip install -e .

# Verificar instalación
py2rocket --version
```

### Variables de entorno (.env)

Variables principales de conexión:

- `ROCKET_API_HOST`: URL base de Rocket
- `ROCKET_AUTH_COOKIE`: cookie de autenticación
- `PROJECT_ID`: proyecto por defecto (opcional)
- `ROCKET_VERIFY_SSL`: verificación SSL (`true`/`false`, default `true`)

Control de warnings SSL inseguros:

- `ROCKET_SUPPRESS_INSECURE_REQUEST_WARNING`: suprime `InsecureRequestWarning` cuando SSL está desactivado (`true`/`false`, default `true`)

Log opcional de errores HTTP:

- `ROCKET_HTTP_ERROR_LOG_FILE`: ruta de archivo para guardar errores HTTP (incluye `status_code` y `response_text`). Si está vacío, no se genera log.
  Rotación automática: al llegar a 5MB, el archivo actual se renombra a `<archivo>.1`.

---

## 📚 Uso Rápido

### 1. Crear un nuevo workflow

```bash
py2rocket create mi-pipeline \
    --params '{"P_TABLA": "ventas.datos"}' \
    --description "Pipeline de ventas"
```

Genera `mi-pipeline.py` con la estructura básica.

### 2. Editar el workflow

```python
from py2rocket import pipeline, sql, print_step

@pipeline(
    name="mi-pipeline",
    execution_engine="Hybrid",
    params={"P_TABLA": "ventas.datos"}
)
def workflow():
    tabla = sql(
        name="Load_Ventas",
        query="SELECT * FROM {{P_TABLA}}",
        priority=50
    )
    print_step(tabla, priority=50)
```

### 3. Compilar a JSON

```bash
py2rocket build mi-pipeline.py -o mi_pipeline.json
```

### 4. Desplegar a Rocket (próximamente)

```bash
py2rocket push mi_pipeline.json \
    --url https://rocket.mycompany.com \
    --token $ROCKET_API_TOKEN
```

---

## 📖 Comandos Disponibles

### `create` - Crear nuevo workflow

```bash
py2rocket create <nombre> [opciones]

Opciones:
  -o, --output PATH         Archivo de salida (default: {nombre}.py)
  -e, --engine ENGINE       Motor: Batch|Streaming|Hybrid (default: Hybrid)
  -p, --params JSON         Parámetros en JSON
  -d, --description TEXT    Descripción del pipeline
```

### `build` - Compilar workflow

```bash
py2rocket build <archivo.py> [opciones]

Opciones:
  -o, --output PATH         Archivo JSON de salida
  -i, --indent NUM          Indentación (default: 2)
```

Nota: el comando `build` formatea automáticamente con `black` los campos
`pythonCode` de nodos PySpark antes de guardar el JSON.
`black` está incluido como dependencia del módulo en `pyproject.toml`.

### `render` - Ver estructura del grafo

```bash
py2rocket render <archivo.py|json> [opciones]

Opciones:
  -o, --output PATH         Archivo JSON de salida (opcional)
  -i, --indent NUM          Indentación (default: 2)
```

Imprime o exporta la estructura del grafo (nodes/edges) sin la configuración completa.
Útil para visualizar la topología del workflow.

### `push` - Desplegar a Rocket

```bash
py2rocket push <archivo.json> --url URL [opciones]

Opciones:
  --url URL                 URL de Rocket (requerido)
  --token TOKEN             Token de API
  --project-id ID           ID del proyecto
  --group-id ID             ID del grupo
  --no-verify-ssl           No verificar SSL
  --dry-run                 Simular sin desplegar
```

> ⚠️ El comando `push` aún no está implementado

### `run` - Ejecutar workflow en Rocket

```bash
py2rocket run <archivo.py|json> [opciones]

Opciones:
  --workflow-id ID          ID del workflow en Rocket (si no se especifica, usa el del archivo)
  --project-id ID           ID del proyecto en Rocket
  --url URL                 URL de Rocket (o usar ROCKET_API_HOST env var)
  --token TOKEN             Cookie de autenticación (o usar ROCKET_AUTH_COOKIE env var)
  --instance INSTANCE       Instancia (default: XS)
  --params-lists JSON       Lista JSON para paramsLists
  --params-lists-file PATH  Ruta a JSON con lista de paramsLists
  --extra-params PATH       Ruta a JSON con lista de extraParams
  --execution-name NAME     Nombre de ejecución
  --execution-description TEXT  Descripción de ejecución
  --execution-priority NUM  Prioridad de ejecución (default: 0)
  --force-execution-if-available-resources  Forzar ejecución si hay recursos
  --retry-unsuccessful-writes               Reintentar escrituras fallidas
  --max-attempts NUM        Máximo de intentos (default: 0)
  --attempts-conditions JSON  Lista JSON con condiciones de reintento
  --extended-audit-info     Habilitar auditoría extendida
  --no-verify-ssl           No verificar SSL
```

Ejecuta un workflow en Rocket. Puede usar un archivo local (.py o .json) o especificar directamente
el workflow-id. Soporta configuración avanzada de ejecución con parámetros, reintentos y auditoría.

### `pull` - Descargar workflow desde Rocket

```bash
py2rocket pull <archivo.py|json> [opciones]

Opciones:
  -o, --output PATH         Archivo de salida (default: mismo nombre que entrada)
  --url URL                 URL de Rocket
  --token TOKEN             Cookie de autenticación
  --no-verify-ssl           No verificar SSL
  -f, --force               Forzar sobrescritura sin preguntar
```

Descarga el workflow desde el servidor usando el `id` del archivo local.

### `download` - Descargar workflow por ID

```bash
py2rocket download <workflow-id> [opciones]

Opciones:
  --token TOKEN             Cookie de autenticación (o ROCKET_AUTH_COOKIE env)
  --no-verify-ssl           No verificar SSL
  -f, --force               Forzar sobrescritura sin preguntar
```

La URL de Rocket se obtiene automáticamente de la variable de entorno `ROCKET_URL`.

Descarga un workflow por su ID (UUID). El nombre del archivo se toma del campo `name` del workflow descargado.
Si el archivo existe, pregunta si desea reemplazarlo o guardarlo con sufijo `_server`.

### `from-json` - Convertir JSON a Python DSL

```bash
py2rocket from-json <archivo.json> [opciones]

Opciones:
  -o, --output PATH         Archivo Python de salida (default: mismo nombre con .py)
```

Convierte un workflow en formato JSON de Rocket al DSL Python de py2rocket.
Características:

- Ordena nodos: Inputs (alfabético) → Transformations → Outputs
- Omite configuración de UI
- Filtra valores por defecto conocidos
- Convierte nombres de parámetros de camelCase a snake_case

### `validate-standard` - Validar estándares del pipeline

```bash
py2rocket validate-standard <archivo.py|json> [opciones]

Opciones:
  -j, --json-output         Mostrar salida en formato JSON en la consola
```

Valida reglas mínimas de estándar sobre un pipeline (desde `.json` o `.py`):

- El pipeline debe tener `description` no vacía.
- Todos los nodos deben tener `description` no vacía.
- No pueden existir prioridades repetidas entre nodos.

Comportamiento de salida:

- **Exit code 0** si cumple todas las reglas.
- **Exit code 1** si hay incumplimientos o error de validación.

Ejemplos:

```bash
# Validación legible
py2rocket validate-standard tests/test_filter_build.json

# Validación para CI/CD con salida estructurada
py2rocket validate-standard tests/test_filter_build.json --json-output
```

### `lint` - Revisar código Python con flake8

```bash
py2rocket lint <archivo_o_carpeta> [opciones]

Opciones:
  --config PATH             Ruta de configuración flake8 (opcional)
  -j, --json-output         Mostrar salida en formato JSON en la consola
  -o, --output PATH         Guardar resultado (texto o JSON con --json-output)
```

`flake8` está incluido como dependencia del módulo en `pyproject.toml`.

Comportamiento de salida:

- **Exit code 0** si no hay issues.
- **Exit code 1** si hay issues de lint.
- **Exit code 2** si hay error de ejecución (por ejemplo, flake8 no instalado).

Ejemplos:

```bash
# Revisar un archivo
py2rocket lint py2rocket/core/transformation.py

# Revisar todo el paquete con configuración personalizada
py2rocket lint py2rocket --config .flake8

# Revisar todo el paquete y guardar JSON para CI/CD
py2rocket lint py2rocket --json-output --output lint_report.json
```

Ejemplo recomendado para CI/CD:

```bash
# 1) Ejecutar lint y guardar resultado estructurado
py2rocket lint py2rocket --json-output --output lint_report.json

# 2) Inspeccionar el reporte (PowerShell)
Get-Content lint_report.json
```

Formato de salida JSON (ejemplo):

```json
{
  "path": "py2rocket",
  "issues_count": 2,
  "issues": [
    {
      "file": "py2rocket/core/transformation.py",
      "line": 1056,
      "column": 13,
      "code": "E501",
      "message": "line too long (95 > 88 characters)"
    }
  ]
}
```

Configuración recomendada de `.flake8`:

```ini
[flake8]
max-line-length = 88
extend-ignore = E203, W503
exclude =
    .git,
    __pycache__,
    .venv,
    build,
    dist
```

Uso con configuración explícita:

```bash
py2rocket lint py2rocket --config .flake8 --json-output --output lint_report.json
```

### `sync` - Sincronizar grupo desde Rocket

```bash
py2rocket sync <nombre-grupo> [opciones]

Opciones:
  -o, --output PATH         Carpeta de salida (default: directorio actual)
  --url URL                 URL de Rocket
  --token TOKEN             Cookie de autenticación
  --no-verify-ssl           No verificar SSL
  -f, --force               Forzar sobrescritura de archivos existentes
```

Sincroniza todos los assets/workflows de un grupo (y sus subgrupos) desde Rocket hacia una estructura de carpetas local.

Características:

- Descarga todos los workflows del grupo especificado
- Incluye automáticamente todos los subgrupos
- Crea una estructura de carpetas organizada por grupo y asset
- Descarga todas las versiones de cada asset
- Convierte automáticamente los workflows JSON a Python DSL
- Genera un archivo `.py2rocket` con metadatos del proyecto para identificación

**Archivo .py2rocket:**
El comando `sync` crea automáticamente un archivo `.py2rocket` en la carpeta de salida con información sobre:

- Nombre del proyecto
- Código del proyecto
- Nombre del grupo base sincronizado
- ID del grupo
- Fecha de sincronización

Este archivo permite que extensiones y herramientas identifiquen que la carpeta fue creada mediante sincronización.
Para más información, consulta [docs/ARCHIVO_PY2ROCKET.md](docs/ARCHIVO_PY2ROCKET.md).

### `get-extensions` - Listar extensiones del proyecto

```bash
py2rocket get-extensions [opciones]

Opciones:
  --url URL                 URL de Rocket (o usar ROCKET_API_HOST env var)
  --token TOKEN             Cookie de autenticación (o usar ROCKET_AUTH_COOKIE env var)
  --no-verify-ssl           No verificar SSL
```

Lista todas las extensiones disponibles en un proyecto Rocket.
El comando solicita de forma interactiva el ID del proyecto (o usa PROJECT_ID del .env).
Muestra información como ID, nombre, tipo de extensión y clases personalizadas.

### `create-group` - Crear grupo en Rocket

```bash
py2rocket create-group [nombre] [opciones]

Opciones:
  --project-name NAME       Nombre del proyecto (o usar PROJECT_NAME env var)
  --url URL                 URL de Rocket (o usar ROCKET_API_HOST env var)
  --token TOKEN             Cookie de autenticación (o usar ROCKET_AUTH_COOKIE env var)
  --no-verify-ssl           No verificar SSL
```

Crea un nuevo grupo en Rocket asociado a un proyecto específico.
Si no se proporcionan los argumentos, los solicita de forma interactiva.
Valida que el proyecto existe antes de crear el grupo.

---

## 💻 Uso Programático

```python
from py2rocket import create, build, push

# Crear workflow
create(
    name="mi-pipeline",
    params={"P_TABLA": "datos.tabla"},
    description="Mi pipeline"
)

# Compilar
build(
    workflow_file="mi-pipeline.py",
    output_path="output.json"
)

# Desplegar (cuando esté implementado)
# push(
#     json_file="output.json",
#     rocket_url="https://rocket.example.com",
#     api_token="token"
# )
```

---

## 🔧 API del Módulo

### Operaciones DSL

- `@pipeline(name, execution_engine, params)` - Decorator para definir pipelines
- `sql(name, query, priority)` - Paso de entrada SQL
- `pyspark(name, code, inputs, priority)` - Transformación PySpark
- `print_step(input_step, priority)` - Salida de debug

### Funciones Principales

- `create()` - Crea archivo .py base
- `build()` - Compila a JSON de Rocket
- `push()` - Despliega vía API (no implementado)

Ver [py2rocket/README.md](py2rocket/README.md) para documentación completa.

---

## 🔀 Soporte Multi-Output (Datos Válidos e Inválidos)

Algunos nodos pueden generar múltiples tipos de datos (ej: `Filter` que genera datos válidos Y descartados).

### API Limpia

```python
# Por defecto: VALID_DATA (implícito)
datos = sql(name="Load", query="SELECT ...")
filtro = filter(name="Filter", filter_exp="x > 100", inputs=datos)
resultado = print_step(name="ValidOutput", inputs=filtro)

# Explícito: DISCARDED_DATA (datos rechazados)
descartes = print_step(name="DiscardedOutput", inputs=filtro.discarded)
```

### Parámetro `.discarded`

- **`filtro`** → StepResult → Crea edge con `dataType: "ValidData"`
- **`filtro.discarded`** → StepResultOutput → Crea edge con `dataType: "DiscardedData"`

### JSON Generado

```json
{
  "edges": [
    {
      "origin": "Filter",
      "destination": "ValidOutput",
      "dataType": "ValidData"
    },
    {
      "origin": "Filter",
      "destination": "DiscardedOutput",
      "dataType": "DiscardedData"
    }
  ]
}
```

### Nodos con Multi-Output

- **`filter()`** - ValidData (pasa) vs DiscardedData (rechaza)
- **`trigger()`** - Condiciones válidas vs inválidas
- **`pyspark()`** - Datos válidos vs errores

Ver [MULTI_OUTPUT_IMPLEMENTATION.md](MULTI_OUTPUT_IMPLEMENTATION.md) para detalles técnicos.

---Define un paso de entrada SQL.

**Parámetros:**

- `name`: Nombre único del paso
- `query`: Query SQL (soporta parámetros `{{NOMBRE}}`)
- `priority`: Prioridad de ejecución (menor = antes)
- `cache_table`: Cachear resultado en memoria
- `description`: Descripción del paso

**Ejemplo:**

```python
tabla = sql(
    name="Load_Ventas",
    query="SELECT * FROM {{P_TABLA}} WHERE fecha >= '2024-01-01'",
    priority=10
)
```

### `pyspark(name, code, inputs, priority, ...)`

Define un paso de transformación PySpark.

**Parámetros:**

- `name`: Nombre único del paso
- `code`: Código PySpark a ejecutar
- `inputs`: Paso(s) previo(s) que alimentan esta transformación
- `priority`: Prioridad de ejecución
- `description`: Descripción de la transformación

**Ejemplo:**

```python
filtrado = pyspark(
    name="Filtrar_Activos",
    code="df.filter(col('estado') == 'activo')",
    inputs=tabla
)
```

### `print_step(input_step, priority, ...)`

Define un paso de salida para imprimir datos.

**Parámetros:**

- `input_step`: Paso previo del cual imprimir datos
- `priority`: Prioridad de ejecución
- `print_data`: Imprimir los datos (costoso)
- `print_schema`: Imprimir el schema
- `print_metadata`: Imprimir metadatos (filas, columnas)
- `log_level`: Nivel de log

**Ejemplo:**

```python
print_step(tabla, print_schema=True, print_metadata=True)
```

## 🔧 Clases Principales

### `Pipeline`

Representa un pipeline completo (DAG).

**Atributos:**

- `name`: Nombre único del pipeline
- `execution_engine`: Motor de ejecución (Batch, Streaming, Hybrid)
- `nodes`: Lista de nodos (operaciones)
- `edges`: Lista de conexiones entre nodos
- `parameters`: Parámetros del pipeline

### `Node`

Representa un nodo en el DAG.

**Atributos:**

- `name`: Identificador único
- `step_type`: Tipo (Input, Transform, Output)
- `class_name`: Clase Rocket que implementa el paso
- `execution_engine`: Motor de ejecución
- `priority`: Prioridad de ejecución
- `configuration`: Configuración específica

### `Edge`

Representa una conexión entre nodos.

**Atributos:**

- `origin`: Nombre del nodo origen
- `destination`: Nombre del nodo destino
- `data_type`: Tipo de datos (ValidData, InvalidData)

## 📋 Reglas del DSL

### ✅ Permitido

- DAG arbitrario
- Un nodo con múltiples salidas (fan-out)
- Un nodo con múltiples entradas (fan-in)

### ❌ Prohibido

- Ciclos en el DAG
- Nodos huérfanos (sin conexiones)
- Outputs sin inputs
- Reutilizar variables (shadowing)

## 🌳 Ejemplo Fan-out / Fan-in

```python
@pipeline("ventas_branching")
def flujo():
    # Carga base
    base = sql("SELECT * FROM ventas")

    # Fan-out: dos transformaciones paralelas
    por_region = pyspark(
        "Agrupar_Region",
        "df.groupBy('region').sum('monto')",
        inputs=base
    )
    por_producto = pyspark(
        "Agrupar_Producto",
        "df.groupBy('producto').sum('monto')",
        inputs=base
    )

    # Fan-in: combinar resultados
    combinado = sql(
        "Combinar",
        "SELECT * FROM por_region CROSS JOIN por_producto",
        inputs=[por_region, por_producto]
    )

    print_step(combinado)
```

## 🎯 Beneficios vs Rocket UI

| Aspecto      | Rocket UI        | DSL               |
| ------------ | ---------------- | ----------------- |
| Versionado   | ❌ Manual        | ✅ Git nativo     |
| Testing      | ❌ Limitado      | ✅ Automático     |
| Validaciones | ⚠️ En runtime    | ✅ En compilación |
| Errores      | ❌ Muchos clicks | ✅ Code review    |
| Velocidad    | 🐢 Lento         | 🚀 Rápido         |
| Gobierno     | ✅ Rocket        | ✅ Rocket         |

## 🗺️ Roadmap

### Fase 1 (Actual) ✅

- DSL básico (sql, pyspark, print)
- IR (modelo intermedio)
- Export JSON Rocket

### Fase 2 (Próximo)

- CLI para compilar pipelines
- Validaciones de DAG (ciclos, huérfanos)
- Más operaciones (join, filter, aggregate)

### Fase 3 (Futuro)

- Deploy automático vía API de Rocket
- Testing framework
- Rocket en modo read-only

## 💡 Mensaje Clave

> **No se reemplaza Rocket.**  
> **Se acelera el desarrollo sobre Rocket.**

Rocket sigue siendo el **runtime**, **gobierno** y **auditoría**.  
El DSL es simplemente una forma más eficiente de **definir** pipelines.

## 📖 Recursos

- `resumen_dsl_rocket.md`: Documento completo de diseño
- `dsl_plantilla.py`: Ejemplo de workflow objetivo
- `reusltado_workflow.json`: JSON de salida esperado
- `ejemplo_uso.py`: Ejemplo funcional completo

## 🤝 Contribuir

El DSL está basado en Apache Spark 3.1.1 y la plataforma Stratio Rocket.

Para añadir nuevas operaciones:

1. Definir la clase Node correspondiente en `dsl_classes.py`
2. Crear la función en `dsl_operations.py`
3. Actualizar la documentación

---

**Desarrollado para Stratio Rocket - Apache Spark 3.1.1**
