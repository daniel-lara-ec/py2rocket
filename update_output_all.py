#!/usr/bin/env python3
"""
Script inteligente para actualizar output.py
"""

with open("py2rocket/core/output.py", "r") as f:
    content = f.read()

# Reemplazos a hacer - patrón específico para cada función
replacements = [
    # custom_lite_xd_output
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
            _attach_outputs_writer(input_step, node.name, "Overwrite")
    else:
        edge = Edge(
            origin=inputs.node.name,
            destination=node.name,
            data_type=DataRelation.VALID_DATA,
        )
        pipeline.add_edge(edge)
        _attach_outputs_writer(inputs, node.name, "Overwrite")

    return StepResult(node, pipeline)


def jdbc_output(""",
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
            _attach_outputs_writer(input_step, node.name, "Overwrite")
    else:
        origin_name, data_relation = _get_origin_and_relation(inputs)
        edge = Edge(
            origin=origin_name,
            destination=node.name,
            data_type=data_relation,
        )
        pipeline.add_edge(edge)
        _attach_outputs_writer(inputs, node.name, "Overwrite")

    return StepResult(node, pipeline)


def jdbc_output(""",
    ),
]

# Aplicar reemplazos
for old, new in replacements:
    if old in content:
        content = content.replace(old, new)
        print(f"✅ Reemplazo aplicado")
    else:
        print(f"⚠️  Patrón no encontrado")

with open("py2rocket/core/output.py", "w") as f:
    f.write(content)

print("✅ Actualización completada")
