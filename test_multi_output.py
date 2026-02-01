#!/usr/bin/env python3
"""
Test para verificar que el soporte multi-output funciona correctamente.

Testa que:
1. filter() retorna StepResult (default VALID_DATA)
2. filter().discarded retorna StepResultOutput (INVALID_DATA)
3. Cuando pasamos ambos a otros nodos, se crean 2 edges con data_type correcto
4. El JSON generado tiene 2 edges con "ValidData" y "DiscardedData"
"""

import json
from py2rocket.core import pipeline, filter
from py2rocket.core.output import print_step
from py2rocket.core.input import sql
from py2rocket.core.pipeline import DataRelation


@pipeline(name="test_multi_output", description="Test multi-output data relations")
def test_multi_output_pipeline():
    """Pipeline que prueba el soporte multi-output con Filter"""

    # Crear un input SQL
    datos = sql(name="SourceData", query="SELECT * FROM tabla_test")

    # Usar filter() - retorna StepResult (VALID_DATA por defecto)
    filtro = filter(name="SepararDatos", filter_exp="cantidad > 100", inputs=datos)

    # Acceder a .discarded - retorna StepResultOutput (INVALID_DATA)
    datos_invalidos = filtro.discarded

    # Verificar que los tipos son correctos
    print(f"Type of filtro: {type(filtro).__name__}")
    print(f"Type of filtro.discarded: {type(datos_invalidos).__name__}")

    # Verificar que tienen el node correcto
    assert filtro.node.name == "SepararDatos", "StepResult debe tener el node correcto"
    assert (
        datos_invalidos.node.name == "SepararDatos"
    ), "StepResultOutput debe tener el node correcto"

    # Verificar data_relation
    from py2rocket.core.pipeline import StepResult, StepResultOutput

    assert isinstance(filtro, StepResult), "filter() debe retornar StepResult"
    assert isinstance(
        datos_invalidos, StepResultOutput
    ), ".discarded debe retornar StepResultOutput"
    assert (
        datos_invalidos.data_relation == DataRelation.INVALID_DATA
    ), ".discarded debe tener INVALID_DATA"

    # Crear output para ambas salidas
    valid_output = print_step(
        name="ValidData_Output",
        inputs=filtro,  # Usa StepResult directamente (VALID_DATA)
    )

    invalid_output = print_step(
        name="InvalidData_Output",
        inputs=datos_invalidos,  # Usa StepResultOutput (INVALID_DATA)
    )

    return valid_output, invalid_output


if __name__ == "__main__":
    # Obtener el pipeline
    p = test_multi_output_pipeline()

    print("\n=== Pipeline creado exitosamente ===\n")

    # Compilar a JSON
    from py2rocket.core.compiler import RocketCompiler

    compiler = RocketCompiler(p)
    json_output = compiler.compile()

    # Mostrar JSON formateado
    print("=== JSON generado ===")
    print(json.dumps(json_output, indent=2))

    # Verificar que hay 2 edges desde "SepararDatos"
    edges = json_output.get("pipelineGraph", {}).get("edges", [])
    separator_edges = [e for e in edges if e["origin"] == "SepararDatos"]

    print(f"\n=== Verificación ===")
    print(f"Total edges: {len(edges)}")
    print(f"Edges desde 'SepararDatos': {len(separator_edges)}")

    # Verificar data types
    for edge in separator_edges:
        print(
            f"  Edge: {edge['origin']} -> {edge['destination']}, dataType: {edge.get('dataType', 'N/A')}"
        )

    # Verificar que hay un edge con ValidData y otro con DiscardedData
    data_types = [e.get("dataType") for e in separator_edges]

    assert "ValidData" in data_types, "Debe haber un edge con ValidData"
    assert "DiscardedData" in data_types, "Debe haber un edge con DiscardedData"

    print("\n=== Test multi-output PASSED! ===")
    print("✓ Filter returns StepResult (VALID_DATA by default)")
    print("✓ Filter.discarded returns StepResultOutput (INVALID_DATA)")
    print("✓ Two edges created with correct dataTypes: ValidData and DiscardedData")
