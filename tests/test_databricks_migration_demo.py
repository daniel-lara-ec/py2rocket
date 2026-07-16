import json
from pathlib import Path

import pytest

from examples import databricks_migration_demo as demo


PROJECTS = [
    {"id": "project-1", "name": "Ventas", "normalizedName": "/ventas"},
    {"id": "project-2", "name": "Riesgos", "normalizedName": "/riesgos"},
]


@pytest.mark.parametrize("value", ["project-2", "Riesgos", "/riesgos"])
def test_select_project_accepts_id_name_or_normalized_name(value):
    assert demo.select_project(PROJECTS, value) == PROJECTS[1]


def test_build_group_path_accepts_relative_path_and_rejects_other_project():
    assert demo.build_group_path(PROJECTS[0], "diarios/cierre") == (
        "/ventas/diarios/cierre"
    )
    assert demo.build_group_path(PROJECTS[0], "") == "/ventas"

    with pytest.raises(ValueError, match="no pertenece"):
        demo.build_group_path(PROJECTS[0], "/riesgos/diarios")


def test_convert_workflows_preserves_hierarchy(tmp_path, monkeypatch):
    downloads = tmp_path / "rocket"
    source = downloads / "ventas" / "asset-a" / "v0.py"
    source.parent.mkdir(parents=True)
    source.write_text("def workflow():\n    pass\n", encoding="utf-8")
    (source.parent / "asset").write_text("{}", encoding="utf-8")

    calls = []

    def fake_build_databricks(**kwargs):
        calls.append(kwargs)
        Path(kwargs["output_path"]).write_text("# notebook", encoding="utf-8")

    monkeypatch.setattr(demo, "build_databricks", fake_build_databricks)
    output = tmp_path / "databricks"
    converted, errors = demo.convert_workflows(
        downloads, output, "unity_catalog.json", {"enabled": True}
    )

    expected = output / "ventas" / "asset-a" / "v0.py"
    assert converted == [expected]
    assert errors == []
    assert expected.read_text(encoding="utf-8") == "# notebook"
    assert calls == [
        {
            "workflow_file": str(source),
            "output_path": str(expected),
            "unity_catalog_mapping_file": "unity_catalog.json",
            "template_replacement": {"enabled": True},
        }
    ]


def test_migration_notebook_is_valid_and_all_code_cells_compile():
    notebook_path = (
        Path(__file__).parents[1] / "examples" / "databricks_migration_demo.ipynb"
    )
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))

    assert notebook["nbformat"] == 4
    code_cells = [cell for cell in notebook["cells"] if cell["cell_type"] == "code"]
    assert "parameters" in code_cells[0]["metadata"]["tags"]
    parameters = "".join(code_cells[0]["source"])
    assert 'load_dotenv()' in parameters
    assert 'os.getenv("ROCKET_AUTH_COOKIE")' in parameters
    assert 'os.getenv("PY2ROCKET_MIGRATION_PROJECT"' in parameters
    assert 'os.getenv("PY2ROCKET_TEMPLATE_NODES"' in parameters

    for index, cell in enumerate(code_cells, start=1):
        compile("".join(cell["source"]), f"<notebook-cell-{index}>", "exec")
