#!/usr/bin/env python3
"""
Script inteligente para actualizar output.py functions
Busca bloques específicos y los actualiza cuidadosamente
"""

with open("py2rocket/core/output.py", "r", encoding="utf-8") as f:
    content = f.read()

# Define los reemplazos necesarios - funciones que todavía no han sido actualizadas
# Buscamos el patrón específico de cada función

replacements = [
    # jdbc_output
    (
        """    pipeline.add_node(node)

    # Manejar múltiples inputs
    if isinstance(inputs, list):
        for input_step in inputs:
            edge = Edge(
                origin=input_step.node.name,
                destination=node.name,
                data_type=DataRelation.VALID_DATA,
            )
            pipeline.add_edge(edge)
    else:
        edge = Edge(
            origin=inputs.node.name,
            destination=node.name,
            data_type=DataRelation.VALID_DATA,
        )
        pipeline.add_edge(edge)

    return StepResult(node, pipeline)


def postgres_output(""",
        """    pipeline.add_node(node)

    # Manejar múltiples inputs
    if isinstance(inputs, list):
        for input_step in inputs:
            origin_name, data_relation = _get_origin_and_relation(input_step)
            edge = Edge(
                origin=origin_name,
                destination=node.name,
                data_type=data_relation,
            )
            pipeline.add_edge(edge)
    else:
        origin_name, data_relation = _get_origin_and_relation(inputs)
        edge = Edge(
            origin=origin_name,
            destination=node.name,
            data_type=data_relation,
        )
        pipeline.add_edge(edge)

    return StepResult(node, pipeline)


def postgres_output(""",
    ),
]

for old, new in replacements:
    if old in content:
        content = content.replace(old, new)
        print(f"✓ Reemplazo aplicado")
    else:
        print(f"× Patrón no encontrado (posiblemente ya actualizado)")

with open("py2rocket/core/output.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Actualización completada")
