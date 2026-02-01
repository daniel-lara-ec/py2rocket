# Detalla el flujo de inicialización de un pipeline.

Se requieren los siguientes datos:

1. name (obligatorio puede se cualquier con un límite de 175 caracteres) este nombre se debe colocar en el objeto pipeline
2. description (opcional, no se requiere en el comando pero puede colocarse en el código después)
3. groupId (obligatorio, Este dato requiere conectase con la API, corresponde a un uuid en base a un string que proporiciona el usuario)
4. projectId (obligatorio, se requiere conectase con la api para convertir el string a uuid)
5. executionEngine (obligatorio, por default y hasta que se implemente otro solo se utiliza Hybrid)

Para lanzar el proceso se deben seguir los siguientes pasos.

1. El usuario proporciona los siguientes datos (ejemplo):
   - name: "pl-transformacion-prueba"
   - description: "Asset para probar la ejecución"
   - groupName: "/home/cda-sandbox/dalarana/pruebas/assets/"
   - projectName: "cda-sandbox"
   - executionEngine: "Hybrid"

   Comando recomendado:

   ```bash
   python -m py2rocket create pl-transformacion-prueba \
      --description "Asset para probar la ejecución" \
      --project-name "cda-sandbox" \
      --group-name "/home/cda-sandbox/dalarana/pruebas/assets/"
   ```

   Si necesitas crear sin verificación de API:

   ```bash
   python -m py2rocket create pl-transformacion-prueba \
      --description "Asset para probar la ejecución" \
      --project-name "cda-sandbox" \
      --group-name "/home/cda-sandbox/dalarana/pruebas/assets/" \
      --offline
   ```

   ⚠️ En modo offline, el pipeline debe ser configurado y subido manualmente en Rocket.

2. La aplicación se conecta con la api (GET) HOST/projects/findByName/{param} y en {param} indica el projectName, como resultado exitoso (código 200), se devuelve un diccionario en el que tomamos el id como projectId

   Si el resultado es erróneo el proceso se para y se indica al usuario que no se encontró el proyecto.

3. La aplicación se conecta a la api HOST/groups/findByName con (GET) y con parámetro name igual al groupName dado por el usuario. Como resultado existoso (200) devuelve un diccionar con un id que es el uuid correspondiente al groupId.

4. Con los datos necesarios completos nos conectamos a la api con GET en HOST/assets y enviamos el siguiente diccionario

{
"workflowAsset": {
"name" : <nombre>,
"description" : <descripción (puede ser un string vacío)>,
"groupId" : <uuid grupo>,
"projectId" : <proyecto uuid>,
"executionEngine" : <tipo ejecución>,
}
}

Como repsuesta exitosa nos va a devolver un diccionario del tipo

{
"workflowAsset" : {
"id": <uuid del asset>,
...
}
}

Tomamos el uuid del asset y lo guardamos en el .py a construir junto con el resto de variables.

5. Con los datos anteriores creamos el pipeline template e indicamos al usuario que puede comenzar a editar.
