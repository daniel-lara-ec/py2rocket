#!/usr/bin/env python3
"""
Script mejorado para actualizar todas las funciones de output.py
Usa un enfoque más seguro que preserva indentación
"""

with open("py2rocket/core/output.py", "r") as f:
    lines = f.readlines()

# Procesar línea por línea, buscando patrones específicos
i = 0
while i < len(lines):
    line = lines[i]

    # Buscar "for input_step in inputs:" dentro de una función output
    # (diferente de transformation que usa "for input_step in input_list:")
    if "for input_step in inputs:" in line:
        indent_match = len(line) - len(line.lstrip())
        indent = " " * indent_match

        # Siguiente línea debe ser "edge = Edge("
        if i + 1 < len(lines) and "edge = Edge(" in lines[i + 1]:
            # Insertar el helper después del for
            helper_line = f"{indent}    origin_name, data_relation = _get_origin_and_relation(input_step)\n"
            lines.insert(i + 1, helper_line)
            i += 1

            # Ahora buscar y actualizar las líneas de edge
            j = i + 1
            while j < len(lines) and "pipeline.add_edge(edge)" not in lines[j]:
                if "origin=input_step.node.name," in lines[j]:
                    lines[j] = lines[j].replace(
                        "origin=input_step.node.name,", "origin=origin_name,"
                    )
                if "data_type=DataRelation.VALID_DATA," in lines[j]:
                    lines[j] = lines[j].replace(
                        "data_type=DataRelation.VALID_DATA,", "data_type=data_relation,"
                    )
                j += 1

    # Buscar "origin=inputs.node.name," cuando sea single input
    elif (
        "origin=inputs.node.name," in line
        and "_get_origin_and_relation" not in "".join(lines[max(0, i - 5) : i])
    ):
        # Esta es la rama else para single input
        # Encontrar cuánta indentación tiene y buscar hacia atrás
        indent_match = len(line) - len(line.lstrip())
        indent = " " * indent_match

        # Insertar helper antes del edge
        helper_line = (
            f"{indent}origin_name, data_relation = _get_origin_and_relation(inputs)\n"
        )
        lines.insert(i, helper_line)
        i += 1

        # Actualizar la línea
        lines[i] = lines[i].replace("origin=inputs.node.name,", "origin=origin_name,")
        lines[i] = lines[i].replace(
            "data_type=DataRelation.VALID_DATA,", "data_type=data_relation,"
        )

    i += 1

# Escribir el archivo
with open("py2rocket/core/output.py", "w") as f:
    f.writelines(lines)

print("Actualizaciones completadas")
