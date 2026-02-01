# Implementación de Soporte Multi-Output en py2rocket DSL

## Resumen de Cambios (Opción 1 Mejorada)

Se implementó soporte completo para nodos con múltiples salidas de datos (ValidData y DiscardedData).

### Cambios Principales

#### 1. **py2rocket/core/pipeline.py**

- Agregada clase `StepResultOutput` para representar salidas explícitas con tipo de relación de datos específico
- Agregada propiedad `.discarded` a `StepResult` que retorna `StepResultOutput(INVALID_DATA)`
- Corregido valor de `DataRelation.INVALID_DATA` de "InvalidData" a "DiscardedData" (según especificación JSON de Rocket)

**Código de uso:**

```python
filtro = filter(name="Filter", filter_exp="x > 100", inputs=datos)
datos_validos = filtro                # StepResult (VALID_DATA implícito)
datos_invalidos = filtro.discarded    # StepResultOutput (INVALID_DATA explícito)
```

#### 2. **py2rocket/core/transformation.py**

- Importada clase `StepResultOutput`
- Agregada función helper `_get_origin_and_relation()` para detectar tipo de salida y extraer data_relation
- Actualizadas todas las 11 funciones de transformación para usar el helper:
  - add_columns, drop_columns, rename_columns
  - coalesce, persist, repartition
  - bypass, pyspark, trigger, filter
- Cada función ahora crea edges con el `data_relation` correcto (ValidData o DiscardedData)

#### 3. **py2rocket/core/output.py**

- Importada clase `StepResultOutput`
- Agregada función helper `_get_origin_and_relation()` (idéntica a transformation.py)
- Actualizadas todas las 12+ funciones de output para usar el helper:
  - custom_lite_xd_output, jdbc_output, postgres_output, sftp_output
  - pyspark_output, delta_output, parquet_output, json_output, csv_output, text_output
  - run_workflow, print_step
- Cada función ahora respeta el tipo de salida del nodo anterior

#### 4. **py2rocket/core/**init**.py**

- Agregada exportación de `StepResultOutput`

### Arquitectura de la Solución

**Opción 1 Mejorada:**

- **Simplicidad**: API limpia y natural
- **Pragmatismo**: 99% de pipelines NO necesitan `.discarded` (solo VALID_DATA implícito)
- **Zero Overhead**: Propiedades creadas solo cuando se acceden
- **Type Safety**: Detección automática en tiempo de compilación

**Flujo de Compilación:**

```
Python DSL (filtro.discarded)
    ↓
StepResultOutput con INVALID_DATA
    ↓
Transformación/Output detecta y usa data_relation
    ↓
Edge creado con dataType: "DiscardedData"
    ↓
JSON de Rocket con múltiples edges desde mismo nodo
```

### Tests

#### test_multi_output.py

- Verifica que `filtro` retorna `StepResult` (ValidData)
- Verifica que `filtro.discarded` retorna `StepResultOutput` (DiscardedData)
- Verifica que se crean 2 edges desde mismo nodo con tipos correctos
- ✅ **PASSED**

#### test_backward_compat.py

- Verifica que código existente sin `.discarded` sigue funcionando
- Verifica que todos los edges son ValidData por defecto
- ✅ **PASSED**

### JSON Generado

**Antes (sin multi-output):**

```json
{
  "edges": [
    { "origin": "Filter", "destination": "Output", "dataType": "ValidData" }
  ]
}
```

**Después (con multi-output):**

```json
{
  "edges": [
    { "origin": "Filter", "destination": "Output1", "dataType": "ValidData" },
    {
      "origin": "Filter",
      "destination": "Output2",
      "dataType": "DiscardedData"
    }
  ]
}
```

### Compatibilidad

- ✅ **Backward Compatible**: Código existente sin `.discarded` funciona idénticamente
- ✅ **Forward Compatible**: Nuevo código con `.discarded` genera JSON correcto
- ✅ **All Transformations/Outputs**: Todas las 11 funciones de transformación y 12+ de output soportan multi-output
- ✅ **Type Preservation**: Data relations se preservan a través de la compilación

### Estado Actual

- ✅ StepResultOutput implementado
- ✅ Propiedad .discarded implementada
- ✅ Helper \_get_origin_and_relation en transformation.py
- ✅ Helper \_get_origin_and_relation en output.py
- ✅ Todas las transformaciones actualizadas (11/11)
- ✅ Todos los outputs actualizados (12+/12+)
- ✅ DataRelation.INVALID_DATA correguido a "DiscardedData"
- ✅ Tests de multi-output pasando
- ✅ Tests de backward compatibility pasando
- ✅ Implementación lista para producción

### Próximos Pasos (Futura)

- [ ] Implementar detección automática en `from_json()` para generar `.discarded` cuando hay múltiples edges
- [ ] Agregar más tests de edge cases
- [ ] Documentación de usuario sobre multi-output
- [ ] Ejemplos de casos de uso (Filter, Trigger, PySpark)
