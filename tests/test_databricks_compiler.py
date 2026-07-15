import re
from pathlib import Path

import pytest

from py2rocket import build_databricks
from py2rocket.core import filter as filter_step
from py2rocket.core import pipeline, print_step, sql
from py2rocket.core.databricks_compiler import (
    DatabricksCompileError,
    DatabricksCompiler,
)
from py2rocket.core.pipeline import (
    DataRelation,
    Edge,
    ExecutionEngine,
    Node,
    Pipeline,
    StepType,
)


def node(name, step_type, class_name, configuration=None, priority=50):
    return Node(
        name=name,
        step_type=step_type,
        class_name=class_name,
        class_pretty_name=class_name,
        execution_engine=ExecutionEngine.BATCH,
        configuration=configuration or {},
        priority=priority,
    )


def test_compiles_one_cell_per_node_and_unity_catalog_tables():
    pipeline = Pipeline(name="ventas", parameters={"P_MIN": "10"})
    pipeline.nodes = [
        node("Load Sales", StepType.INPUT, "SQLInputStep", {"query": "SELECT * FROM old.sales"}, 30),
        node("Active Sales", StepType.TRANSFORMATION, "FilterTransformStep", {"filterExp": "amount > {{P_MIN}}"}, 20),
        node("Save Sales", StepType.OUTPUT, "DeltaOutputStep", {"saveMode": "Overwrite"}, 10),
    ]
    pipeline.edges = [
        Edge("Load Sales", "Active Sales"),
        Edge("Active Sales", "Save Sales"),
    ]

    notebook = DatabricksCompiler(
        pipeline,
        {
            "sources": {"Load Sales": "main.bronze.sales"},
            "destinations": {"Save Sales": {"table": "main.silver.sales"}},
        },
    ).compile()

    assert notebook.startswith("# Databricks notebook source")
    assert notebook.count("# Node:") == 3
    assert "load_sales = spark.table('main.bronze.sales')" in notebook
    assert "active_sales = load_sales.filter(_condition)" in notebook
    assert ".saveAsTable('main.silver.sales')" in notebook
    assert notebook.index("# Node: Load Sales") < notebook.index("# Node: Active Sales")


def test_discarded_edge_uses_discarded_dataframe():
    pipeline = Pipeline(name="discarded")
    pipeline.nodes = [
        node("source", StepType.INPUT, "SQLInputStep", {"query": "SELECT 1 AS ok"}),
        node("filter", StepType.TRANSFORMATION, "FilterTransformStep", {"filterExp": "ok = 1"}),
        node("show", StepType.OUTPUT, "PrintOutputStep", {"printData": True}),
    ]
    pipeline.edges = [
        Edge("source", "filter"),
        Edge("filter", "show", DataRelation.INVALID_DATA),
    ]

    notebook = DatabricksCompiler(pipeline).compile()
    assert "filter__discarded = source.filter" in notebook
    assert "display(filter__discarded)" in notebook


def test_cycle_is_rejected():
    pipeline = Pipeline(name="cycle")
    pipeline.nodes = [
        node("a", StepType.TRANSFORMATION, "ByPassStep"),
        node("b", StepType.TRANSFORMATION, "ByPassStep"),
    ]
    pipeline.edges = [Edge("a", "b"), Edge("b", "a")]

    with pytest.raises(DatabricksCompileError, match="cycle"):
        DatabricksCompiler(pipeline).compile()


INPUT_CLASSES = {
    "CustomLiteXDInputStep": {"customLiteClassType": "custom.reader"},
    "SFTPInputStep": {"path": "/in", "host": "sftp"},
    "TestInputStep": {"event": "x", "numEvents": "1"},
    "SQLInputStep": {"query": "SELECT 1"},
    "JdbcInputStep": {"url": "jdbc:test", "dbtable": "t"},
    "PostgresInputStep": {"url": "jdbc:postgresql:test", "dbtable": "t"},
    "PySparkInputStep": {"pythonCode": "spark.range(1)"},
    "ParquetInputStep": {"path": "/in"},
    "DeltaInputStep": {"path": "/in"},
    "JsonInputStep": {"path": "/in"},
    "CsvInputStep": {"path": "/in"},
    "FileSystemInputStep": {"path": "/in"},
}

TRANSFORM_CLASSES = {
    "AddColumnsTransformStep": {"addColumnExpressionList": []},
    "DropColumnsTransformStep": {"schema.fields": []},
    "SelectTransformStep": {"columns": "id"},
    "DistinctTransformStep": {},
    "DropDuplicatesTransformStep": {"columns": "id"},
    "RenameColumnTransformationStep": {"columns": []},
    "CustomLiteXDTransformStep": {"customLiteClassType": "custom.transform"},
    "CoalesceTransformStep": {"partitions": "1"},
    "PersistTransformStep": {},
    "RepartitionTransformStep": {},
    "ByPassStep": {},
    "PySparkTransformerStep": {"pythonCode": "df"},
    "TriggerTransformStep": {"sql": "SELECT * FROM source"},
    "FilterTransformStep": {"filterExp": "id > 0"},
    "UnionTransformStep": {},
    "MlModelTransformStep": {"MlModelAux": "models:/demo/1"},
}

OUTPUT_CLASSES = {
    "CustomLiteXDOutputStep": {"customLiteClassType": "custom.writer"},
    "JdbcOutputStep": {"url": "jdbc:test", "dbtable": "t"},
    "PostgresOutputStep": {"url": "jdbc:postgresql:test", "dbtable": "t"},
    "SFTPOutputStep": {"path": "/out", "host": "sftp"},
    "PrintOutputStep": {"printData": True},
    "RunWorkflowOutputStep": {"workflowId": "/Shared/child"},
    "PySparkOutputStep": {"pythonCode": "df.count()"},
    "DeltaOutputStep": {"path": "/out"},
    "ParquetOutputStep": {"path": "/out"},
    "JsonOutputStep": {"path": "/out"},
    "CsvOutputStep": {"path": "/out"},
    "TextOutputStep": {"path": "/out"},
}


def test_coverage_matrix_matches_every_dsl_node_class():
    core = Path(__file__).parents[1] / "py2rocket" / "core"
    declared = set()
    for filename in ("input.py", "transformation.py", "output.py"):
        source = (core / filename).read_text(encoding="utf-8")
        declared.update(re.findall(r'class_name="([^"]+)"', source))

    covered = set(INPUT_CLASSES) | set(TRANSFORM_CLASSES) | set(OUTPUT_CLASSES)
    assert covered == declared


@pytest.mark.parametrize("class_name,configuration", INPUT_CLASSES.items())
def test_every_input_class_compiles(class_name, configuration):
    pipeline = Pipeline(name=class_name)
    pipeline.nodes = [node("input", StepType.INPUT, class_name, configuration)]
    notebook = DatabricksCompiler(pipeline).compile()
    compile(notebook, "<databricks-notebook>", "exec")


@pytest.mark.parametrize("class_name,configuration", TRANSFORM_CLASSES.items())
def test_every_transformation_class_compiles(class_name, configuration):
    pipeline = Pipeline(name=class_name)
    pipeline.nodes = [
        node("source", StepType.INPUT, "SQLInputStep", {"query": "SELECT 1 id"}),
        node("transform", StepType.TRANSFORMATION, class_name, configuration),
    ]
    pipeline.edges = [Edge("source", "transform")]
    notebook = DatabricksCompiler(pipeline).compile()
    compile(notebook, "<databricks-notebook>", "exec")


@pytest.mark.parametrize("class_name,configuration", OUTPUT_CLASSES.items())
def test_every_output_class_compiles(class_name, configuration):
    pipeline = Pipeline(name=class_name)
    pipeline.nodes = [node("output", StepType.OUTPUT, class_name, configuration)]
    if class_name != "RunWorkflowOutputStep":
        pipeline.nodes.insert(
            0, node("source", StepType.INPUT, "SQLInputStep", {"query": "SELECT 1"})
        )
        pipeline.edges = [Edge("source", "output")]
    notebook = DatabricksCompiler(pipeline).compile()
    compile(notebook, "<databricks-notebook>", "exec")


def test_public_api_builds_notebook_from_dsl(tmp_path):
    @pipeline(name="public-api", params={"P_MIN": "10"})
    def workflow():
        source = sql(name="Load", query="SELECT * FROM old.sales")
        selected = filter_step(
            name="Filter", filter_exp="amount > {{P_MIN}}", inputs=source
        )
        print_step(name="Show", inputs=selected, print_data=True)

    output = tmp_path / "workflow_databricks.py"
    result = build_databricks(
        pipeline_obj=workflow(),
        output_path=str(output),
        unity_catalog_mapping={"Load": "main.bronze.sales"},
    )

    assert result == str(output)
    assert output.read_text(encoding="utf-8").count("# Node:") == 3
