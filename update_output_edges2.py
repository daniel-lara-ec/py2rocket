#!/usr/bin/env python3
"""
Script para actualizar todas las funciones de output para usar _get_origin_and_relation
Maneja ambos patrones de iteración
"""

import re

# Leer el archivo
with open("py2rocket/core/output.py", "r") as f:
    content = f.read()

# Patrón 1: Cuando está dentro de isinstance(inputs, list)
pattern1_old = r"""(\s+)if isinstance\(inputs, list\):
(\s+)for input_step in inputs:
(\s+)edge = Edge\(
(\s+)origin=input_step\.node\.name,
(\s+)destination=node\.name,
(\s+)data_type=DataRelation\.VALID_DATA,
(\s+)\)
(\s+)pipeline\.add_edge\(edge\)"""

pattern1_new = r"""\1if isinstance(inputs, list):
\2for input_step in inputs:
\3    origin_name, data_relation = _get_origin_and_relation(input_step)
\3    edge = Edge(
\4        origin=origin_name,
\5        destination=node.name,
\6        data_type=data_relation,
\7    )
\8    pipeline.add_edge(edge)"""

updated_content = re.sub(pattern1_old, pattern1_new, content)

# Contar reemplazos
count1 = len(re.findall(pattern1_old, content))
print(f"Patrón 1 (isinstance): {count1} reemplazos")

# Patrón 2: else (cuando input es single)
pattern2_old = r"""(\s+)else:
(\s+)edge = Edge\(
(\s+)origin=inputs\.node\.name,
(\s+)destination=node\.name,
(\s+)data_type=DataRelation\.VALID_DATA,
(\s+)\)
(\s+)pipeline\.add_edge\(edge\)"""

pattern2_new = r"""\1else:
\2    origin_name, data_relation = _get_origin_and_relation(inputs)
\2    edge = Edge(
\3        origin=origin_name,
\4        destination=node.name,
\5        data_type=data_relation,
\6    )
\7    pipeline.add_edge(edge)"""

updated_content = re.sub(pattern2_old, pattern2_new, updated_content)

# Contar reemplazos
count2 = len(re.findall(pattern2_old, updated_content))
print(f"Patrón 2 (else single): {count2} reemplazos")

print(f"Total reemplazos: {count1 + count2}")

# Escribir el archivo actualizado
with open("py2rocket/core/output.py", "w") as f:
    f.write(updated_content)

print("Archivo actualizado exitosamente")
