#!/usr/bin/env python3
"""
Script para actualizar todas las funciones de output para usar _get_origin_and_relation
"""

import re

# Leer el archivo
with open("py2rocket/core/output.py", "r") as f:
    content = f.read()

# Patrón a buscar: el bloque de creación de edges con input_step.node.name
old_pattern = r"""(\s+)for input_step in input_list:
(\s+)edge = Edge\(
(\s+)origin=input_step\.node\.name,
(\s+)destination=name,
(\s+)data_type=DataRelation\.VALID_DATA,
(\s+)\)
(\s+)pipeline\.add_edge\(edge\)"""

new_pattern = r"""\1for input_step in input_list:
\1    origin_name, data_relation = _get_origin_and_relation(input_step)
\1    edge = Edge(
\1        origin=origin_name,
\1        destination=name,
\1        data_type=data_relation,
\1    )
\1    pipeline.add_edge(edge)"""

# Hacer el reemplazo
updated_content = re.sub(old_pattern, new_pattern, content)

# Contar cuántos reemplazos se hicieron
count = len(re.findall(old_pattern, content))
print(f"Reemplazos realizados: {count}")

# Escribir el archivo actualizado
with open("py2rocket/core/output.py", "w") as f:
    f.write(updated_content)

print("Archivo actualizado exitosamente")
