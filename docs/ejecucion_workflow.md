# Permite ejecutar el workflow

Una vez cargado el workflow, puedes ejecutarlo con el comando `run`. Internamente se envía un POST al endpoint `HOST/workflows/runWithExecutionContext` con los headers/cookies habituales y el siguiente payload base:

```json
{
  "projectId": "196d1c2d-5afd-4756-ba37-80aa58d0f742",
  "workflowId": "23cb821c-83f6-44f6-880a-6b409d08e76c",
  "executionContext": {
    "paramsLists": ["Environment", "SparkConfigurations"],
    "extraParams": []
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

## Parámetros estrictamente necesarios

- `projectId` (CLI: `--project-id` o `PROJECT_ID` en .env)
- `workflowId` (CLI: `--workflow-id` o `id` dentro del JSON compilado)
- `rocket_url` (CLI: `--url` o `ROCKET_API_HOST` en .env)
- `api_token` (CLI: `--token` o `ROCKET_AUTH_COOKIE` en .env)

## Parámetros opcionales soportados

### Contexto de ejecución

- `paramsLists`: se extrae del JSON en `settings.global.parametersLists`.
  - Se puede sobreescribir con `--params-lists` (lista JSON) o `--params-lists-file` (archivo JSON).
- `instance`: se agrega por defecto a `paramsLists` (default: `XS`).
- `extraParams`: lista de diccionarios con formato `{ "name": "P_FECHA_DESDE", "value": "YYYY-MM-DD" }`.
  - Se pasa con `--extra-params` apuntando a un archivo JSON.

### Ajustes de ejecución

- `execution_name` (CLI: `--execution-name`)
- `execution_description` (CLI: `--execution-description`)
- `execution_priority` (CLI: `--execution-priority`)
- `force_execution_if_available_resources` (CLI: `--force-execution-if-available-resources`)
- `retry_unsuccessful_writes` (CLI: `--retry-unsuccessful-writes`)
- `max_attempts` (CLI: `--max-attempts`)
- `attempts_conditions` (CLI: `--attempts-conditions` con lista JSON)
- `extended_audit_info` (CLI: `--extended-audit-info`)

## Ejemplos

### Ejecución mínima

```bash
py2rocket run mi_workflow.json \
	--project-id 196d1c2d-5afd-4756-ba37-80aa58d0f742 \
	--workflow-id 23cb821c-83f6-44f6-880a-6b409d08e76c \
	--url https://rocket.example.com \
	--token "<cookie>"
```

### Ejecución con paramsLists y extraParams

```bash
py2rocket run mi_workflow.json \
	--project-id 196d1c2d-5afd-4756-ba37-80aa58d0f742 \
	--workflow-id 23cb821c-83f6-44f6-880a-6b409d08e76c \
	--url https://rocket.example.com \
	--token "<cookie>" \
	--params-lists "[\"Environment\", \"SparkConfigurations\"]" \
	--instance XS \
	--extra-params extraParams.json
```
