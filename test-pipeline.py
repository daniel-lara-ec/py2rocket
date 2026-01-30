"""
Pipeline de prueba

Workflow generado por py2rocket
"""

from py2rocket import pipeline, sql, print_step


@pipeline(
    name="test-pipeline",
    execution_engine="Hybrid",
    params={'P_TABLA': 'test.tabla'}
)
def workflow():
    """
    Define el flujo de procesamiento del pipeline.
    
    Ejemplo:
        tabla = sql(
            name="Load_Tabla",
            query="SELECT * FROM {{P_TABLA}}",
            priority=50
        )
        
        print_step(tabla, priority=50)
    """
    # TODO: Implementa tu pipeline aquí
    pass


if __name__ == "__main__":
    from py2rocket import build
    
    # Construir el pipeline
    pipe = workflow()
    
    # Compilar a JSON
    build(pipe, "test_pipeline.json")
