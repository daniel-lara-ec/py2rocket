# Reorganización del Módulo Operations

## Resumen

El módulo `operations.py` ha sido reorganizado en tres módulos especializados siguiendo una arquitectura más clara y mantenible:

- **`input.py`**: Operaciones de entrada de datos
- **`transformation.py`**: Operaciones de transformación
- **`output.py`**: Operaciones de salida

## Estructura Anterior

```
py2rocket/core/
  ├── operations.py    # Todas las operaciones juntas
  ├── pipeline.py
  ├── decorators.py
  └── compiler.py
```

## Estructura Nueva

```
py2rocket/core/
  ├── input.py          # Operaciones de entrada (NEW)
  ├── transformation.py # Operaciones de transformación (NEW)
  ├── output.py         # Operaciones de salida (NEW)
  ├── operations.py     # Compatibilidad hacia atrás
  ├── pipeline.py
  ├── decorators.py
  └── compiler.py
```

## Operaciones por Módulo

### 📥 input.py - Operaciones de Entrada

Operaciones que leen datos desde fuentes externas:

#### `sql()`

Ejecuta queries SQL sobre fuentes de datos configuradas en Rocket.

**Ejemplo:**

```python
ventas = sql(
    name="Load_Ventas",
    query="SELECT * FROM {{P_TABLA}} WHERE fecha >= '2024-01-01'",
    priority=10
)
```

#### `csv()` ⭐ NUEVO

Lee archivos CSV desde el sistema de archivos.

**Ejemplo:**

```python
datos = csv(
    name="Load_CSV",
    path="/data/ventas.csv",
    header=True,
    delimiter=","
)
```

### 🔄 transformation.py - Operaciones de Transformación

Operaciones que transforman datos entre pasos:

#### `pyspark()`

Ejecuta código PySpark personalizado para transformar datos.

**Ejemplo:**

```python
filtrado = pyspark(
    name="Filtrar_Activos",
    code="df.filter(col('estado') == 'activo')",
    inputs=base
)
```

#### `repartition()` ⭐ NUEVO

Reparticiona el DataFrame para optimizar la distribución de datos.

**Ejemplo:**

```python
particionado = repartition(
    name="Repartition_Data",
    inputs=base,
    partitions="10",
    columns="fecha"
)
```

### 📤 output.py - Operaciones de Salida

Operaciones que escriben o emiten datos:

#### `print_step()`

Imprime información del DataFrame para debugging.

**Ejemplo:**

```python
print_step(tabla, print_schema=True)
```

#### `run_workflow()` ⭐ NUEVO

Ejecuta otro workflow como parte del pipeline.

**Ejemplo:**

```python
run_workflow(
    name="Execute_Process",
    inputs=tabla,
    workflow_id="workflow-123",
    run_workflow_when="RECEIVE_DATA"
)
```

## Compatibilidad Hacia Atrás

El archivo `operations.py` original ahora actúa como un módulo de compatibilidad que reexporta todas las funciones de los nuevos módulos especializados.

**Código existente sigue funcionando:**

```python
from py2rocket.core import sql, pyspark, print_step  # ✅ Funciona
```

**También puedes importar desde los módulos específicos:**

```python
from py2rocket.core.input import sql, csv
from py2rocket.core.transformation import pyspark, repartition
from py2rocket.core.output import print_step, run_workflow
```

## Nuevas Transformaciones Agregadas

Basadas en las referencias en `docs/ref/`:

1. **CSV Input** (Csv_Input.json)
   - Lectura de archivos CSV
   - Soporte para múltiples rutas
   - Filtros glob y regex
   - Configuración de esquema flexible

2. **Repartition Transformation** (Repartition_Transformation.json)
   - Reparticionamiento de DataFrames
   - Por número de particiones
   - Por columnas específicas

3. **Run Workflow Output** (Runworkflow_Output.json)
   - Ejecución de workflows anidados
   - Paso de variables y contextos
   - Control de reintentos
   - Prioridad de ejecución

## Ventajas de la Reorganización

1. **📂 Mejor Organización**: Código agrupado por tipo de operación
2. **🔍 Mayor Claridad**: Más fácil encontrar operaciones específicas
3. **🔧 Mantenibilidad**: Más simple agregar nuevas operaciones
4. **📚 Documentación**: Cada módulo tiene su propósito bien definido
5. **✅ Compatibilidad**: El código existente sigue funcionando sin cambios

## Migración Recomendada

Aunque no es necesario, se recomienda migrar gradualmente:

```python
# Antes
from py2rocket.core import sql, pyspark, print_step

# Después (recomendado)
from py2rocket.core.input import sql, csv
from py2rocket.core.transformation import pyspark, repartition
from py2rocket.core.output import print_step, run_workflow
```

## Testing

Todos los tests existentes pasan sin modificaciones:

```bash
$ python test-pipeline.py
✓ Pipeline compilado: test_pipeline.json
  - Nombre: test-pipeline
  - Nodos: 0
  - Edges: 0
  - Motor: Hybrid
```
