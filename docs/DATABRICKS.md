# Compilación a Databricks

`py2rocket` puede generar un notebook Databricks Source (`.py`) preservando una
celda por cada nodo del pipeline.

```powershell
py2rocket build-databricks pipeline.py `
  --unity-catalog-map unity_catalog.json `
  -o pipeline_databricks.py
```

El mapping de Unity Catalog es opcional y utiliza los nombres de los nodos:

```json
{
  "sources": {
    "Load_Ventas": "main.bronze.ventas",
    "Load_Clientes": {"table": "main.bronze.clientes"},
    "Load_JDBC": {
      "url": "jdbc:postgresql://host/database",
      "dbtable": "public.orders",
      "user": "service_user",
      "password_secret": {"scope": "migration", "key": "jdbc-password"}
    }
  },
  "transformations": {
    "Apply_Model": {
      "model_uri": "models:/fraud_detector/Production",
      "feature_columns": ["amount", "country"],
      "result_type": "double"
    },
    "Custom_Transform": {
      "adapter": "company.custom.Transform"
    }
  },
  "destinations": {
    "Save_Result": "main.silver.ventas_clientes",
    "Call_Child": {
      "notebook": "/Shared/child_pipeline",
      "timeout_seconds": 3600
    }
  }
}
```

- Una entrada SQL mapeada se genera como `spark.table(...)`.
- Una entrada SQL sin mapping se genera como `spark.sql(...)`.
- Un output mapeado se genera como `saveAsTable(...)`.
- Los parámetros `{{P_X}}` y `{{{P_X}}}` se convierten usando widgets.
- Las ramas `DiscardedData` usan una variable con sufijo `__discarded`.

Los bloques PySpark de una sola expresión se asignan automáticamente al resultado.
Un bloque de varias líneas debe dejar el DataFrame final en `result`, `output` o
`df`.

## Cobertura

El compilador cubre todas las clases que actualmente producen las funciones públicas
de `input.py`, `transformation.py` y `output.py`: SQL, JDBC, PostgreSQL, SFTP,
archivos, Delta, PySpark, Test, Custom Lite XD, transformaciones tabulares, Trigger,
ML Model, outputs de archivo/base de datos y Run Workflow.

JDBC, SFTP y Custom Lite XD pueden necesitar librerías instaladas en el cluster y
opciones adicionales en el mapping. Los Custom Lite XD de transformación se resuelven
mediante el registro generado `PY2ROCKET_ADAPTERS`:

```python
def custom_transform(spark, dfs, configuration):
    return dfs[0].transform(my_company_transform)

PY2ROCKET_ADAPTERS["company.custom.Transform"] = custom_transform
```

Si no se registra el adaptador, el notebook conserva el DataFrame de entrada y emite
una advertencia. De esta manera el notebook sigue siendo ejecutable y el punto de
migración pendiente queda explícito.
