# Demo de conversión a Databricks

La carpeta contiene los tres artefactos de una conversión completa:

- `ventas_pipeline.py`: pipeline escrito con el DSL de py2rocket.
- `unity_catalog.json`: correspondencia entre nodos y tablas de Unity Catalog.
- `ventas_databricks.py`: notebook Databricks Source generado.

Para regenerar el notebook:

```powershell
py2rocket build-databricks examples/databricks_demo/ventas_pipeline.py `
  --unity-catalog-map examples/databricks_demo/unity_catalog.json `
  -o examples/databricks_demo/ventas_databricks.py
```

El ejemplo demuestra parámetros con widgets, lectura desde Unity Catalog, cálculo
de columnas, filtro con ramas válidas y descartadas, escritura a una tabla y una
celda por cada nodo del DAG.
