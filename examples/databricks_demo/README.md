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

## Migración completa desde Rocket

Para descargar todos los workflow assets de un proyecto y convertir todas sus
versiones a notebooks Databricks Source, ejecuta desde la raíz del repositorio:

```bash
python -m examples.databricks_migration_demo
```

La demo consulta los proyectos disponibles y solicita el proyecto, la ruta del
grupo en Rocket (vacía para migrar todo el proyecto) y la ruta local de salida.
Los DSL descargados quedan bajo `rocket/` y los notebooks bajo `databricks/`,
con la misma jerarquía de grupos, assets y versiones. Requiere
`ROCKET_API_HOST` y `ROCKET_AUTH_COOKIE` en el entorno o en `.env`.

También se puede ejecutar sin preguntas:

```bash
python -m examples.databricks_migration_demo \
  --project /proyecto-ventas \
  --group-path pipelines/diarios \
  --output migracion_ventas \
  --unity-catalog-map examples/databricks_demo/unity_catalog.json
```

### Notebook Jupyter paso a paso

El archivo `examples/databricks_migration_demo.ipynb` contiene el mismo proceso
separado por celdas. La primera celda de código concentra toda la parametría y
está etiquetada como `parameters`, por lo que también puede sobrescribirse con
Papermill. Ábrelo desde la raíz del repositorio para que estén disponibles los
módulos `py2rocket` y `examples`.

La parametría se lee desde el archivo `.env` de la raíz:

```dotenv
ROCKET_API_HOST=https://rocket.example.com
ROCKET_AUTH_COOKIE=your_auth_cookie
ROCKET_VERIFY_SSL=true
PY2ROCKET_MIGRATION_PROJECT=/proyecto-ventas
PY2ROCKET_MIGRATION_GROUP_PATH=pipelines/diarios
PY2ROCKET_MIGRATION_OUTPUT=migracion_databricks
PY2ROCKET_UNITY_CATALOG_MAP=examples/databricks_demo/unity_catalog.json
PY2ROCKET_MIGRATION_FORCE=false
```

La plantilla completa está en `.env.example`. Si
`PY2ROCKET_MIGRATION_PROJECT` está vacío, el notebook muestra los proyectos y
solicita una selección; un grupo vacío migra el proyecto completo.
