# py2rocket

<div align="center">

🚀 **DSL Python para generar pipelines de Stratio Rocket**

[![Version](https://img.shields.io/badge/version-0.1.0-blue.svg)](https://github.com/yourusername/py2rocket)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

</div>

---

## 🎯 ¿Qué es py2rocket?

**py2rocket** es un DSL (Domain-Specific Language) en Python que permite definir pipelines de **Stratio Rocket** de forma declarativa, eliminando la dependencia de la UI para desarrollo.

### Beneficios

✅ **Versionado en Git** - Pipelines como código  
✅ **Testing automático** - Valida antes de desplegar  
✅ **Desarrollo más rápido** - Menos clicks, más código  
✅ **Code Review** - Revisa cambios como cualquier PR  
✅ **Reutilización** - Componentes compartidos

> **Importante**: Rocket sigue siendo el **runtime**, **gobierno** y **auditoría**.  
> py2rocket solo acelera el desarrollo.

---

## 📦 Instalación

```bash
# Desde el repositorio
pip install -e .

# O copiar el módulo directamente
cp -r py2rocket /tu/proyecto/
```

---

## 🚀 Inicio Rápido

### 1. Crear un nuevo workflow

```bash
py2rocket create mi-pipeline \
    --params '{"P_TABLA": "ventas.datos"}' \
    --description "Pipeline de procesamiento de ventas"
```

Esto genera `mi-pipeline.py`:

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

if __name__ == "__main__":
    from py2rocket import build
    pipe = workflow()
    build(pipe, "mi_pipeline.json")
```

### 2. Compilar a JSON de Rocket

```bash
py2rocket build mi-pipeline.py -o mi_pipeline.json
```

### 3. Desplegar a Rocket (próximamente)

```bash
py2rocket push mi_pipeline.json \
    --url https://rocket.mycompany.com \
    --token $ROCKET_API_TOKEN \
    --project-id abc-123
```

### 4. Validar estándar de descripción y prioridades

```bash
py2rocket validate-standard mi_pipeline.json
```

Salida estructurada para CI/CD:

```bash
py2rocket validate-standard mi_pipeline.json --json-output
```

Reglas validadas:

- El pipeline debe tener `description` no vacía.
- Todos los nodos deben tener `description` no vacía.
- No deben existir prioridades repetidas entre nodos.

Código de salida:

- `0` si la validación es correcta.
- `1` si hay incumplimientos o errores de validación.

---

## 📚 Uso Programático

### Crear workflow

```python
from py2rocket import create

create(
    name="pl-ventas-diarias",
    params={"P_FECHA": "2024-01-01"},
    description="Pipeline de ventas diarias"
)
# Genera: pl-ventas-diarias.py
```

### Definir pipeline

```python
from py2rocket import pipeline, sql, pyspark, print_step

@pipeline(
    name="pl-transformacion-ventas",
    execution_engine="Hybrid",
    params={"P_TABLA": "ventas.data"}
)
def mi_workflow():
    # 1. Cargar datos
    ventas = sql(
        name="Load_Ventas",
        query="SELECT * FROM {{P_TABLA}} WHERE fecha >= '2024-01-01'",
        priority=10
    )

    # 2. Transformar con PySpark
    filtrado = pyspark(
        name="Filtrar_Activos",
        code="df.filter(col('estado') == 'activo')",
        inputs=ventas,
        priority=20
    )

    # 3. Imprimir resultados
    print_step(filtrado, print_schema=True, priority=30)
```

### Compilar

```python
from py2rocket import build

# Desde objeto
pipe = mi_workflow()
build(pipe, "output.json")

# Desde archivo
build(workflow_file="mi_workflow.py", output_path="output.json")
```

---

## 🔧 API Reference

### `create(name, output_path, execution_engine, params, description)`

Crea un archivo .py base para un nuevo workflow.

**Parámetros:**

- `name` (str): Nombre del pipeline
- `output_path` (str, opcional): Ruta del archivo de salida
- `execution_engine` (str): "Batch", "Streaming" o "Hybrid" (default)
- `params` (dict, opcional): Parámetros del pipeline
- `description` (str, opcional): Descripción del pipeline

**Retorna:** `str` - Ruta del archivo creado

---

### `build(pipeline_obj, output_path, workflow_file, indent)`

Compila un workflow Python a JSON de Rocket.

**Parámetros:**

- `pipeline_obj` (Pipeline, opcional): Objeto Pipeline a compilar
- `output_path` (str, opcional): Ruta del JSON de salida
- `workflow_file` (str, opcional): Archivo .py con el workflow
- `indent` (int): Indentación del JSON (default: 2)

**Retorna:** `str` - Ruta del archivo JSON generado

---

### `push(json_file, rocket_url, api_token, project_id, ...)`

Despliega un pipeline a Stratio Rocket vía API.

> ⚠️ **Función no implementada aún**

**Parámetros:**

- `json_file` (str): Ruta al JSON del pipeline
- `rocket_url` (str): URL de Rocket
- `api_token` (str, opcional): Token de API
- `project_id` (str, opcional): ID del proyecto
- `group_id` (str, opcional): ID del grupo
- `verify_ssl` (bool): Verificar SSL (default: True)
- `dry_run` (bool): Simular sin desplegar (default: False)

**Retorna:** `dict` - Resultado del despliegue

---

## 🏗️ Operaciones Disponibles

### `sql(name, query, priority, ...)`

Define un paso de entrada SQL.

```python
tabla = sql(
    name="Load_Tabla",
    query="SELECT * FROM {{P_TABLA}} WHERE active = 1",
    priority=50,
    cache_table=False
)
```

### `pyspark(name, code, inputs, priority, ...)`

Define un paso de transformación PySpark.

```python
filtrado = pyspark(
    name="Filtrar_Datos",
    code="df.filter(col('precio') > 100)",
    inputs=tabla,
    priority=60
)
```

### `print_step(input_step, priority, ...)`

Define un paso de salida para debug.

```python
print_step(
    filtrado,
    print_schema=True,
    print_metadata=True,
    priority=70
)
```

---

## 📂 Estructura del Proyecto

```
py2rocket/
├── __init__.py              # API pública (create, build, push)
├── cli.py                   # CLI de comandos
├── core/
│   ├── __init__.py
│   ├── pipeline.py          # Clases: Pipeline, Node, Edge
│   ├── operations.py        # Operaciones: sql, pyspark, print
│   ├── decorators.py        # Decorator @pipeline
│   └── compiler.py          # Compilador IR → JSON
└── templates/
    └── workflow_template.py # Plantilla para create
```

---

## 🎯 Principios de Diseño

- ✅ El DSL **NO ejecuta datos**
- ✅ El DSL **DESCRIBE un DAG**
- ✅ Rocket **EJECUTA y audita**
- ✅ El pipeline es la **fuente de verdad**

---

## 🛣️ Roadmap

### ✅ Fase 1 (Actual)

- DSL básico (sql, pyspark, print)
- Comandos create y build
- Compilador a JSON de Rocket

### 🚧 Fase 2 (En progreso)

- Comando push con API de Rocket
- Validaciones de DAG (ciclos, huérfanos)
- Más operaciones (join, union, filter)

### 📋 Fase 3 (Futuro)

- Testing framework
- Templates corporativos
- Integración CI/CD

---

## 📖 Ejemplos

Ver carpeta `ejemplos/` para más casos de uso:

- `ejemplo_basico.py` - Pipeline simple
- `ejemplo_branching.py` - Fan-out / Fan-in
- `ejemplo_parametros.py` - Uso de parámetros

---

## 🤝 Contribuir

Las contribuciones son bienvenidas!

1. Fork el proyecto
2. Crea una rama (`git checkout -b feature/nueva-operacion`)
3. Commit cambios (`git commit -am 'Añade nueva operación'`)
4. Push a la rama (`git push origin feature/nueva-operacion`)
5. Abre un Pull Request

---

## 📄 Licencia

MIT License - Ver [LICENSE](LICENSE) para más detalles

---

## 🙏 Agradecimientos

Desarrollado para **Stratio Rocket** basado en **Apache Spark 3.1.1**

---

<div align="center">
  
**¿Preguntas? Abre un [issue](https://github.com/yourusername/py2rocket/issues)**

</div>
