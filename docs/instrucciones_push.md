# Instrucciones para implementar el push al servicio una vez se ha creado el pipeline

Una vez generado el pipeline y construido (ya tenemos el json) podemos utilizar el comando push para cargarlo al aplicativo registrado. Para ello realizaremos los siguientes pasos.

1. Al compilar el pipeline el id del asset debemos incluirlo en el json como workflowMasterId

2. Conectamos con la api HOST/workflows y en una operación PUT enviamos como contenido el json generado.
