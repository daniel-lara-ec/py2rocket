#!/usr/bin/env python3
"""
Script masivo para actualizar TODAS las funciones de output
"""

import re

with open("py2rocket/core/output.py", "r", encoding="utf-8") as f:
    content = f.read()

# Reemplazo global: todas las instancias de los patrones
# Pero tiene que ser cuidadoso para no romper nada

# Patrón 1: En bucles for
pattern1 = r"(for input_step in inputs:)\n(\s+)edge = Edge\(\n(\s+)origin=input_step\.node\.name,\n(\s+)destination=node\.name,\n(\s+)data_type=DataRelation\.VALID_DATA,"

replacement1 = r"\1\n\2origin_name, data_relation = _get_origin_and_relation(input_step)\n\2edge = Edge(\n\3origin=origin_name,\n\4destination=node.name,\n\5data_type=data_relation,"

content = re.sub(pattern1, replacement1, content)
print("✓ Patrón 1 actualizado (for loops)")

# Patrón 2: Else blocks
pattern2 = r"(else:)\n(\s+)edge = Edge\(\n(\s+)origin=inputs\.node\.name,\n(\s+)destination=node\.name,\n(\s+)data_type=DataRelation\.VALID_DATA,"

replacement2 = r"\1\n\2origin_name, data_relation = _get_origin_and_relation(inputs)\n\2edge = Edge(\n\3origin=origin_name,\n\4destination=node.name,\n\5data_type=data_relation,"

content = re.sub(pattern2, replacement2, content)
print("✓ Patrón 2 actualizado (else blocks)")

with open("py2rocket/core/output.py", "w", encoding="utf-8") as f:
    f.write(content)

print("\nActualización completada!")
