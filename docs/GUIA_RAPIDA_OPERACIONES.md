# Guía Rápida: Nuevas Operaciones

## 📥 Operaciones de Entrada

### SQL - Ejecutar Queries

```python
from py2rocket.core.input import sql

tabla = sql(
    name="Load_Data",
    query="SELECT * FROM tabla WHERE fecha = '{{{P_FECHA}}}'",
    priority=10,
    cache_table=True
)
```

### CSV - Leer Archivos ⭐ NUEVO

```python
from py2rocket.core.input import csv

datos = csv(
    name="Load_CSV",
    path="/data/archivo.csv",
    header=True,
    delimiter=",",
    priority=10,
    is_recursive_enabled=True
)
```

## 🔄 Operaciones de Transformación

### PySpark - Transformar Datos

```python
from py2rocket.core.transformation import pyspark

resultado = pyspark(
    name="Transform",
    code="df.filter(col('cantidad') > 100)",
    inputs=tabla,
    priority=20
)
```

### Repartition - Optimizar Distribución ⭐ NUEVO

```python
from py2rocket.core.transformation import repartition

optimizado = repartition(
    name="Optimize",
    inputs=resultado,
    partitions="10",
    columns="fecha",
    priority=30
)
```

## 📤 Operaciones de Salida

### Print - Debuggear Datos

```python
from py2rocket.core.output import print_step

print_step(
    input_step=optimizado,
    print_schema=True,
    print_metadata=True
)
```

### Run Workflow - Workflows Anidados ⭐ NUEVO

```python
from py2rocket.core.output import run_workflow

run_workflow(
    name="ExecuteNext",
    inputs=optimizado,
    workflow_id="workflow-123",
    run_workflow_when="RECEIVE_DATA",
    forward_variables=True
)
```

## 🎯 Ejemplo Completo

```python
from py2rocket.core.input import sql, csv
from py2rocket.core.transformation import pyspark, repartition
from py2rocket.core.output import print_step, run_workflow
from py2rocket.core import pipeline, RocketCompiler

@pipeline(
    name="mi-pipeline",
    execution_engine="Hybrid"
)
def crear_pipeline():
    # Entrada
    datos_sql = sql(
        name="Load_SQL",
        query="SELECT * FROM ventas",
        priority=10
    )

    datos_csv = csv(
        name="Load_CSV",
        path="/data/clientes.csv",
        header=True,
        priority=10
    )

    # Transformación
    unido = pyspark(
        name="Join",
        code="df_0.join(df_1, 'id')",
        inputs=[datos_sql, datos_csv],
        priority=20
    )

    optimizado = repartition(
        name="Optimize",
        inputs=unido,
        partitions="10",
        priority=30
    )

    # Salida
    print_step(optimizado)

    run_workflow(
        name="Next",
        inputs=optimizado,
        workflow_id="next-workflow"
    )

# Compilar
pipe = crear_pipeline()
compiler = RocketCompiler(pipe)
compiler.save("output.json")
```

## 📚 Referencia Completa de Parámetros

### csv()

- `name` (str): Nombre único del paso
- `path` (str): Ruta al archivo/directorio
- `delimiter` (str): Delimitador (defecto: ",")
- `header` (bool): Primera fila es header
- `priority` (int): Prioridad de ejecución
- `path_glob_filter` (str): Patrón glob (defecto: "\*.csv")
- `is_recursive_enabled` (bool): Buscar recursivamente
- `metadata_column_enabled` (bool): Incluir columnas de metadatos
- `data_as_json_enabled` (bool): Habilitar lectura como JSON

### repartition()

- `name` (str): Nombre único del paso
- `inputs` (StepResult | list): Paso(s) anterior(es)
- `partitions` (str): Número de particiones deseadas
- `columns` (str): Columnas por las que particionar
- `priority` (int): Prioridad de ejecución
- `description` (str): Descripción del paso

### run_workflow()

- `name` (str): Nombre único del paso
- `inputs` (StepResult | list): Paso(s) anterior(es)
- `workflow_id` (str): ID del workflow a ejecutar
- `asset_id` (str): ID del asset
- `run_workflow_when` (str): Cuándo ejecutar
- `variables` (str): Variables a pasar
- `contexts` (str): Contextos a pasar
- `forward_variables` (bool): Reenviar variables
- `forward_contexts` (bool): Reenviar contextos
- `max_attempts` (int): Número máximo de intentos
- `priority` (int): Prioridad de ejecución

## 🔗 Importaciones Recomendadas

```python
# Opción 1: Desde módulos específicos (RECOMENDADO)
from py2rocket.core.input import sql, csv
from py2rocket.core.transformation import pyspark, repartition
from py2rocket.core.output import print_step, run_workflow

# Opción 2: Desde core (compatible)
from py2rocket.core import sql, csv, pyspark, repartition, print_step, run_workflow

# Opción 3: Desde operations (compatible)
from py2rocket.core.operations import sql, csv, pyspark, repartition, print_step, run_workflow
```

## 💡 Consejos

1. **Prioridades**: Usa números menores para ejecutar primero
   - Entrada: 10-20
   - Transformación: 30-50
   - Salida: 60-80

2. **Nombres descriptivos**: Usa convenciones claras
   - `Load_` para entrada
   - `Transform_` o `Filter_` para transformación
   - Verbo al inicio para salidas

3. **Parametrización**: Usa `{{PARAMETRO}}` para valores dinámicos
   - En queries SQL
   - En rutas de archivos
   - En IDs de workflows

4. **Reparticionamiento**: Agrégalo después de transformaciones costosas
   - Mejora performance en salidas
   - Reduce número de archivos finales

5. **Debugging**: Usa `print_step()` para validar transformaciones
   - Con `print_schema=True` para ver estructura
   - Con `print_metadata=True` para estadísticas

## ❓ Preguntas Frecuentes

**P: ¿Qué pasó con el archivo operations.py?**
R: Sigue existiendo pero ahora solo reexporta funciones para compatibilidad. Las operaciones están en input.py, transformation.py y output.py

**P: ¿Mi código antiguo seguirá funcionando?**
R: Sí, 100% compatible hacia atrás.

**P: ¿Cuándo debo usar csv() vs sql()?**
R: Usa `csv()` para archivos, `sql()` para bases de datos.

**P: ¿Qué hace repartition()?**
R: Redistribuye los datos en más/menos particiones para optimizar performance.

**P: ¿run_workflow() es como un workflow anidado?**
R: Exacto, permite invocar otros workflows como parte del pipeline.

## 📖 Documentación

- [Documentación completa](REORGANIZACION_OPERATIONS.md)
- [Resumen visual](RESUMEN_REORGANIZACION.md)
- [Ejemplo completo](../ejemplo_nuevas_operaciones.py)
