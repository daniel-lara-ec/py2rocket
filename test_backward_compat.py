#!/usr/bin/env python3
"""
Test rápido para verificar que el DSL aún funciona correctamente
después de la implementación de multi-output
"""

from py2rocket.core import pipeline, sql, add_columns, filter, print_step
from py2rocket.core.pipeline import DataRelation


@pipeline(name="test_backward_compat", description="Test backward compatibility")
def test_simple_pipeline():
    """Pipeline simple para verificar que no se rompió nada"""

    # Input
    datos = sql(name="Load", query="SELECT * FROM tabla")

    # Transform - sin usar multi-output
    con_calc = add_columns(
        name="AddCalc", inputs=datos, add_column_expression_list="total * 2 as doble"
    )

    # Output - pasando StepResult directamente (no .discarded)
    salida = print_step(name="Output", inputs=con_calc)

    return salida


if __name__ == "__main__":
    try:
        p = test_simple_pipeline()
        print("✓ Pipeline creado exitosamente")

        from py2rocket.core.compiler import RocketCompiler

        compiler = RocketCompiler(p)
        json_data = compiler.compile()
        print("✓ JSON compilado exitosamente")

        # Verificar que hay edges
        edges = json_data.get("pipelineGraph", {}).get("edges", [])
        print(f"✓ {len(edges)} edges creados")

        # Verificar que todos son ValidData (backward compatibility)
        for edge in edges:
            assert (
                edge["dataType"] == "ValidData"
            ), f"Edge debería ser ValidData, es {edge['dataType']}"

        print("✓ Todos los edges son ValidData (backward compatible)")
        print("\n✅ Test backward compatibility PASSED!")

    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback

        traceback.print_exc()
