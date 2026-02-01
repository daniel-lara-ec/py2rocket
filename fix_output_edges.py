#!/usr/bin/env python3
"""
Script para actualizar todas las funciones de output para usar _get_origin_and_relation
Maneja ambos patrones de iteración sin romper indentación
"""

# Leer el archivo
with open("py2rocket/core/output.py", "r") as f:
    lines = f.readlines()

# Primero, agregar la importación de StepResultOutput
output_lines = []
import_added = False

for i, line in enumerate(lines):
    # Agregar importación si no existe
    if not import_added and "from py2rocket.core.pipeline import (" in line:
        import_added = True
        # Buscar la línea que cierra la importación
        j = i
        while j < len(lines) and ")" not in lines[j]:
            j += 1

        # Agregar StepResultOutput antes del cierre
        if "StepResultOutput" not in "".join(lines[i : j + 1]):
            # Reemplazar la línea que contiene StepResult
            for k in range(i, j + 1):
                if "StepResult," in lines[k]:
                    lines[k] = lines[k].replace(
                        "StepResult,", "StepResult,\n    StepResultOutput,"
                    )
                    break

# Procesar el archivo línea por línea
i = 0
while i < len(lines):
    line = lines[i]

    # Buscar la línea "for input_step in input_list:"
    if "for input_step in input_list:" in line or "for input_step in inputs:" in line:
        # Obtener la indentación
        indent = len(line) - len(line.lstrip())
        indent_str = " " * indent

        # Reemplazar el siguiente bloque
        if i + 1 < len(lines) and "edge = Edge(" in lines[i + 1]:
            # Encontrar el cierre del Edge
            j = i + 1
            while j < len(lines) and "pipeline.add_edge(edge)" not in lines[j]:
                j += 1

            # Insertar la línea de _get_origin_and_relation
            lines.insert(
                i + 1,
                f"{indent_str}    origin_name, data_relation = _get_origin_and_relation(input_step)\n",
            )

            # Actualizar referencias a input_step.node.name
            for k in range(i + 2, j + 2):
                if k < len(lines) and "origin=input_step.node.name," in lines[k]:
                    lines[k] = lines[k].replace(
                        "origin=input_step.node.name,", "origin=origin_name,"
                    )
                if k < len(lines) and "data_type=DataRelation.VALID_DATA," in lines[k]:
                    lines[k] = lines[k].replace(
                        "data_type=DataRelation.VALID_DATA,", "data_type=data_relation,"
                    )

            # Saltar al siguiente
            i = j + 2
            continue

    # Buscar también "origin=inputs.node.name," para single inputs
    elif (
        "origin=inputs.node.name," in line
        and "for input_step" not in lines[max(0, i - 10) : i]
    ):
        indent = len(line) - len(line.lstrip())
        indent_str = " " * indent

        # Obtener contexto - buscar si es dentro de un else
        found_else = False
        for k in range(max(0, i - 5), i):
            if "else:" in lines[k]:
                found_else = True
                break

        if found_else:
            # Insertar _get_origin_and_relation antes del Edge
            lines.insert(
                i,
                f"{indent_str}origin_name, data_relation = _get_origin_and_relation(inputs)\n",
            )
            # Actualizar la línea
            lines[i + 1] = lines[i + 1].replace(
                "origin=inputs.node.name,", "origin=origin_name,"
            )
            lines[i + 1] = lines[i + 1].replace(
                "data_type=DataRelation.VALID_DATA,", "data_type=data_relation,"
            )
            i += 2
            continue

    output_lines.append(line)
    i += 1

# Combinar y escribir
result = "".join(output_lines + lines[len(output_lines) :])

with open("py2rocket/core/output.py", "w") as f:
    f.write("".join(lines))

print("Archivo actualizado")
