# Permite ejecutar el workflow

Una vez cargado el workflow podemos ejecutarlo con el comando run en ese caso, se ejecuta enviando el siguiente diccionario al endpoint HOST/workflows/runWithExecutionContext en modo POST con los headers y cookies usuales con el contenido.

json ```
{
"projectId": "196d1c2d-5afd-4756-ba37-80aa58d0f742",
"workflowId": "23cb821c-83f6-44f6-880a-6b409d08e76c",
"executionContext": {
"paramsLists": [
"Environment",
"SparkConfigurations"
],
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

en este se debe reemplazar el projectID y el workflowId y paramsLists debe ser extraido del json de la clave: parametersLists.

Se debe considerar un parámetro adicional en el comando con el nombre instance y que por defecto es XS,de tipo string y que debe hacer un append a paramsLists.

Finalmente, se debe considerar el parámetro extraParams que debe hacer referencia a un archivo json con una lista de diccionarios de tipo {
"name": "P_FECHA_DESDE",
"value": "YYYY-MM-DD"
}

y que serán pasados a extraParams.
