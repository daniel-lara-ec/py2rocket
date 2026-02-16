# Arquitectura de outputsWriter

## Problema

Los nodos de salida (Output) pueden recibir datos de MÚLTIPLES fuentes (transformaciones). Por lo tanto, los parámetros específicos de escritura (como `tableName`, `partitionBy`, etc.) NO pueden ser propiedades del nodo output, ya que cada fuente podría necesitar configuración diferente.

## Solución: outputsWriter

Los parámetros de escritura se almacenan en el nodo **transformation** (o input), no en el nodo output. Esto se hace mediante el campo `outputsWriter` que es una lista de configuraciones, una por cada salida.

### Estructura en JSON

```json
{
  "name": "Transformacion",
  "outputsWriter": [
    {
      "saveMode": "Overwrite",
      "outputStepName": "Po_Guardado",
      "tableName": "{{{P_NOMBRE_TABLA}}}",
      "discardTableName": "",
      "extraOptions": {
        "partitionBy": "periodo",
        "partitionOverwriteEnabled": true,
        "checkIfEmpty": true,
        "partitionColumns": "",
        "saveMode": "Overwrite",
        "partitions": ""
      }
    }
  ]
}
```

El nodo output **NO** tiene estos parámetros en su `configuration`:

```json
{
  "name": "Po_Guardado",
  "stepType": "Output",
  "configuration": {
    "path": "s3a://...",
    "saveOptions": "",
    "debugOptions": {...}
  }
}
```

## Flujo de Conversión

### JSON → Python (from-json)

1. **Extraer outputsWriter**: El código en `__init__.py` (líneas 1317-1349) extrae los parámetros de `outputsWriter` de cada nodo transformation/input

2. **Inyectar en output**: Los parámetros se inyectan en el `config_args` del nodo output (líneas 1476-1479)

3. **Generar código**: Se genera código Python que pasa estos parámetros a la función de salida:

```python
po_guardado = parquet_output(
    name="Po_Guardado",
    path="s3a://...",
    save_mode="Overwrite",
    table_name="{{{P_NOMBRE_TABLA}}}",
    partition_by="periodo",
    partition_overwrite=True,
    check_if_empty=True,
    inputs=transformacion
)
```

### Python → JSON (compile)

1. **Función de salida**: Las funciones de salida (como `parquet_output`) aceptan los parámetros pero NO los guardan en la configuración del nodo
   - Ver `output.py` líneas 915-923: La config solo tiene `path` y `saveOptions`, NO tiene `tableName`, `partitionBy`, etc.

2. **\_attach_outputs_writer**: Las funciones de salida llaman internamente a `_attach_outputs_writer()` que agrega estos parámetros al `outputs_writer` del nodo **input/transformation**
   - Ver `output.py` líneas 950-1005

3. **Compilación**: Al compilar el pipeline, el campo `outputs_writer` de cada nodo se serializa como `outputsWriter` en el JSON

## Archivos Clave

### `py2rocket/__init__.py`

- **Líneas 1317-1349**: Extracción de outputsWriter del JSON
- **Líneas 1476-1479**: Inyección de parámetros en nodos output

### `py2rocket/core/output.py`

- **Líneas 81-110**: Función `_attach_outputs_writer()` que agrega config a transformation.outputs_writer
- **Líneas 878-1005**: Función `parquet_output()` que:
  - Acepta parámetros de escritura (save_mode, table_name, partition_by, etc.)
  - NO los guarda en node.configuration
  - Llama a `_attach_outputs_writer()` para agregarlos a transformation.outputs_writer

### `py2rocket/core/models.py`

- **Línea ~50**: Clase `Node` tiene el campo `outputs_writer: List[Dict]`

## Ventajas

1. **Múltiples fuentes**: Un output puede recibir datos de varias transformations, cada una con su propia configuración
2. **Separación de conceptos**: El output solo tiene configuración de "dónde escribir", las transformations tienen configuración de "cómo escribir"
3. **DSL limpio**: El DSL en Python esconde esta complejidad - el desarrollador solo pasa parámetros a `parquet_output()`

## Testing

Para verificar que la conversión bidireccional funciona:

```bash
# Generar Python desde JSON
python -m py2rocket from-json ref_workflows/conversion/original.json -o conversion.py

# Ejecutar Python para generar JSON rebuilt
python conversion.py

# Verificar que outputsWriter se preservó correctamente
python test_outputs_writer_roundtrip.py
```

El test verifica que:

- Los parámetros están en transformation.outputsWriter en el rebuilt
- Los parámetros NO están en output.configuration en el rebuilt
- Los valores coinciden con el original
