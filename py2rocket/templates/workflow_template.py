"""
Plantilla base para crear workflows de Stratio Rocket
"""

WORKFLOW_TEMPLATE = '''"""
{description}

Workflow generado por py2rocket
"""

from py2rocket.core import pipeline, sql, print_step


@pipeline(
    name="{name}",
    execution_engine="{engine}",
    params={params},
    project_id={project_id},
    group_id={group_id},
    asset_id={asset_id},
    parameters_lists={parameters_lists},
    pre_execution_sql_sentences={pre_execution_sql_sentences},
    udfs_to_register={udfs_to_register},
    udafs_to_register={udafs_to_register},
    user_spark_conf={user_spark_conf}
)
def workflow():
    """
    Define el flujo de procesamiento del pipeline.
    
    Ejemplo:
        tabla = sql(
            name="Load_Tabla",
            query="SELECT * FROM {{{{P_TABLA}}}}",
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
    build(pipe, "{output_file}")
'''
