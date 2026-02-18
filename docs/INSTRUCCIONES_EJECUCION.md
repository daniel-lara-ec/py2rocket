# INSTRUCCIONES DE EJECUCIÓN (flujo completo y validado)

Este documento explica, de forma operativa y exhaustiva, cómo ejecutar un workflow con `py2rocket` replicando el flujo real de Rocket en **2 pasos**:

1. Consultar parámetros de ejecución (`runWithParametersViewById`).
2. Ejecutar con contexto (`runWithExecutionContext`).

También incorpora el caso que indicaste: **listas de parámetros adicionales** (por ejemplo `ProyectoCertificacion`) que deben incluirse en la ejecución.

---

## 1) Validación del funcionamiento de comandos asociados

Se verificó el funcionamiento de CLI en este workspace con:

```powershell
python -m py2rocket run-view-parameters -h
python -m py2rocket run -h
```

Resultado:

- `run-view-parameters` soporta: `workflow_id`, `--url`, `--token`, `-o`, `-j`, `--no-verify-ssl`.
- `run` soporta: `--workflow-id`, `--project-id`, `--url`, `--token`, `--instance`, `--params-lists`, `--params-lists-file`, `--extra-params` y `executionSettings` (`--execution-name`, `--execution-priority`, etc.).

Además, en el código (`py2rocket/__init__.py`) se confirmó:

- `run-view-parameters` invoca `POST /workflows/runWithParametersViewById/{workflowId}`.
- `run` invoca `POST /workflows/runWithExecutionContext`.
- `run` valida estrictamente `extraParams` como lista de objetos `{"name":"...","value":"..."}`.

---

## 2) Prerrequisitos

Configura `.env` (o pasa flags equivalentes):

- `ROCKET_API_HOST`
- `ROCKET_AUTH_COOKIE`
- `PROJECT_ID` (recomendado)
- `ROCKET_VERIFY_SSL` (opcional)

Verificación rápida:

```powershell
python -m py2rocket projects -j
```

---

## 3) Paso 1: consultar parámetros disponibles

```powershell
python -m py2rocket run-view-parameters <WORKFLOW_ID> -j -o run_params.json
```

Ejemplo:

```powershell
python -m py2rocket run-view-parameters ca8ca3b8-2d96-4f3a-a56c-cd9244f8150b -j -o run_params.json
```

### 3.1 Qué campos debes interpretar

De `run_params.json`:

- `groupsAndContexts`
  - Cada elemento representa una familia de parámetros.
  - Si tiene `contexts`, debes escoger el contexto a ejecutar (ej: `S`, `M`, `XL`).
  - Si **no** tiene `contexts`, se usa el nombre de `parameterList.name`.
- `extraParams`
  - Nombres de parámetros extra obligatorios sin default.
- `extraParamsWithDefault`
  - Parámetros con valor por defecto (pueden venir como objeto tipo mapa, p.ej. `{ "P": "a" }`, o en otros entornos con estructura distinta).

### 3.2 Regla correcta para construir `paramsLists`

Para cada item de `groupsAndContexts`:

1. Si `contexts` está vacío: agregar `parameterList.name`.
2. Si `contexts` tiene elementos: elegir **un** contexto (salvo casos especiales) y agregar su `context.name`.
3. Repetir para todas las familias, incluidas las adicionales del workflow/proyecto.

Ejemplo con tu caso real:

- `Environment` (tiene contextos) → elegir uno: `Production` o `Development` o `PreProduction`.
- `SparkConfigurations` (sin contextos) → usar `SparkConfigurations`.
- `SparkResources` (con contextos) → elegir uno: `S`, `M`, `L`, etc.
- `ProyectoCertificacion` (sin contextos) → usar `ProyectoCertificacion`.

Una construcción válida sería:

```json
["Environment", "SparkConfigurations", "S", "ProyectoCertificacion"]
```

> Nota: en algunos workflows el backend acepta el nombre de lista padre aunque existan contextos, pero **la opción robusta** es pasar el contexto explícito cuando existe.

---

## 4) Paso 2: preparar archivos de ejecución

## 4.1 `params_lists.json`

```json
["Environment", "SparkConfigurations", "S", "ProyectoCertificacion"]
```

## 4.2 `extra_params.json`

Debe ser una lista de objetos `{name, value}`:

```json
[
  { "name": "PARAMETRO", "value": "Valor" },
  { "name": "P", "value": "a" }
]
```

### 4.3 Cómo pasar `extraParamsWithDefault`

Si en la consulta recibes:

```json
"extraParamsWithDefault": { "P": "a" }
```

Para `run` debes convertirlo a lista de objetos al incorporarlo en `extra_params.json`:

```json
{ "name": "P", "value": "a" }
```

Esto es obligatorio porque `run` valida ese formato.

---

## 5) Paso 3: ejecutar workflow

Comando recomendado (archivo para params + extras):

```powershell
python -m py2rocket run pipeline_generado.json `
  --workflow-id ca8ca3b8-2d96-4f3a-a56c-cd9244f8150b `
  --project-id 078e27ea-a99e-4023-9b32-b2da4d116a00 `
  --params-lists-file .\params_lists.json `
  --extra-params .\extra_params.json
```

Si todo va bien:

- estado `success`
- respuesta con identificador de ejecución (UUID)

---

## 6) Relación exacta con el payload HTTP

`py2rocket run` construye y envía:

```json
{
  "projectId": "...",
  "workflowId": "...",
  "executionContext": {
    "paramsLists": ["..."],
    "extraParams": [{ "name": "...", "value": "..." }]
  },
  "executionSettings": {
    "name": "",
    "description": "",
    "executionPriority": 0,
    "forceExecutionIfAvailableResources": false,
    "retryUnsuccessfulWrites": false,
    "maxAttempts": 0,
    "attemptsConditions": [],
    "governanceSettings": {
      "qualityRuleSettings": {
        "extendedAuditInfo": false
      }
    }
  }
}
```

Es equivalente al payload manual que compartiste.

---

## 7) Precedencia de valores (importante)

En `run`, el orden real es:

- `paramsLists`:
  1. `--params-lists`
  2. `--params-lists-file`
  3. `settings.global.parametersLists` del JSON del pipeline
- `project_id`:
  1. `--project-id`
  2. `PROJECT_ID` en `.env`
- `workflow_id`:
  1. `--workflow-id`
  2. `id` del JSON del pipeline
- `rocket_url`:
  1. `--url`
  2. `ROCKET_API_HOST`
- `api_token`:
  1. `--token`
  2. `ROCKET_AUTH_COOKIE`
- `extraParams`:
  1. parámetro en memoria (si se llama función directamente)
  2. `--extra-params` (archivo)
  3. `[]`

Además:

- `--instance` (default `XS`) se añade a `paramsLists` si no existe.

---

## 8) Validaciones y errores comunes

## 8.1 Formato inválido de `extra_params.json`

Error esperado:

- `extraParams debe ser una lista de diccionarios`
- `Cada item de extraParams debe ser un dict con 'name' y 'value'`

## 8.2 Formato inválido de `params_lists`

Error esperado:

- `paramsLists debe ser una lista de strings`

## 8.3 Falta de credenciales o IDs

Error esperado (según faltantes):

- `Faltan parámetros requeridos para ejecutar: project_id, workflow_id, rocket_url, api_token`

## 8.4 Archivos no encontrados

Errores esperados:

- `Archivo no encontrado: ...`
- `Archivo de paramsLists no encontrado: ...`
- `Archivo de extraParams no encontrado: ...`

---

## 9) Checklist obligatorio antes de ejecutar

- Se consultó `run-view-parameters` del workflow actual.
- Se revisó `groupsAndContexts` completo (incluyendo listas adicionales como `ProyectoCertificacion`).
- `params_lists.json` incluye todas las familias requeridas y un contexto por cada familia con contextos.
- `extra_params.json` incluye todos los `extraParams` obligatorios.
- Se añadieron/sobrescribieron defaults de `extraParamsWithDefault` cuando aplique.
- `workflow_id`, `project_id`, `url` y `token` están resueltos por flags o `.env`.

---

## 10) Ejemplo completo final (caso con parámetros adicionales)

Consulta:

```powershell
python -m py2rocket run-view-parameters ca8ca3b8-2d96-4f3a-a56c-cd9244f8150b -j -o run_params.json
```

`params_lists.json`:

```json
["Environment", "SparkConfigurations", "S", "ProyectoCertificacion"]
```

`extra_params.json`:

```json
[
  { "name": "PARAMETRO", "value": "Valor" },
  { "name": "P", "value": "a" }
]
```

Ejecución:

```powershell
python -m py2rocket run pipeline_generado.json `
  --workflow-id ca8ca3b8-2d96-4f3a-a56c-cd9244f8150b `
  --project-id 078e27ea-a99e-4023-9b32-b2da4d116a00 `
  --params-lists-file .\params_lists.json `
  --extra-params .\extra_params.json
```

Este flujo reproduce correctamente la ejecución completa, incluyendo listas y parámetros adicionales.
