"""Compiler from the py2rocket intermediate DAG to a Databricks source notebook."""

from __future__ import annotations

import json
import keyword
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from py2rocket.core.pipeline import DataRelation, Node, Pipeline


def _literal_options(value: Any) -> Dict[str, Any]:
    """Parse Rocket JSON or comma-separated key=value option strings."""
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass
    result: Dict[str, Any] = {}
    for item in value.split(","):
        if "=" in item:
            key, option = item.split("=", 1)
            result[key.strip()] = option.strip()
    return result


def _spark_type_name(value: Any) -> str:
    names = {
        "stringtype": "string",
        "integertype": "int",
        "longtype": "long",
        "doubletype": "double",
        "floattype": "float",
        "booleantype": "boolean",
        "datetype": "date",
        "timestamptype": "timestamp",
    }
    text = str(value or "string").strip()
    return names.get(text.lower(), text.lower())


class DatabricksCompileError(ValueError):
    """Raised when a Rocket node cannot be translated safely."""


class DatabricksCompiler:
    """Generate a Databricks ``.py`` source notebook, preserving one cell per node."""

    CELL_SEPARATOR = "\n\n# COMMAND ----------\n\n"

    def __init__(
        self,
        pipeline: Pipeline,
        unity_catalog_mapping: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.pipeline = pipeline
        self.mapping = dict(unity_catalog_mapping or {})
        self.sources = self._mapping_section("sources")
        self.transformations = self._mapping_section("transformations")
        self.destinations = self._mapping_section("destinations")
        self._variables = self._make_unique_variables(pipeline.nodes)
        self._incoming = defaultdict(list)
        for edge in pipeline.edges:
            self._incoming[edge.destination].append(edge)

    @classmethod
    def from_mapping_file(
        cls, pipeline: Pipeline, mapping_path: Optional[str] = None
    ) -> "DatabricksCompiler":
        mapping: Dict[str, Any] = {}
        if mapping_path:
            path = Path(mapping_path)
            if not path.exists():
                raise FileNotFoundError(f"Unity Catalog mapping not found: {path}")
            mapping = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(mapping, dict):
                raise DatabricksCompileError("Unity Catalog mapping must be a JSON object")
        return cls(pipeline, mapping)

    def _mapping_section(self, name: str) -> Dict[str, Any]:
        section = self.mapping.get(name)
        if isinstance(section, dict):
            return section
        # A flat mapping is accepted as a convenient source-only mapping.
        if name == "sources" and not any(
            key in self.mapping
            for key in ("sources", "transformations", "destinations")
        ):
            return self.mapping
        return {}

    @staticmethod
    def _identifier(value: str) -> str:
        result = re.sub(r"\W+", "_", value, flags=re.UNICODE).strip("_").lower()
        if not result:
            result = "step"
        if result[0].isdigit() or keyword.iskeyword(result):
            result = f"step_{result}"
        return result

    def _make_unique_variables(self, nodes: Iterable[Node]) -> Dict[str, str]:
        used: Dict[str, int] = {}
        result: Dict[str, str] = {}
        for node in nodes:
            base = self._identifier(node.name)
            count = used.get(base, 0) + 1
            used[base] = count
            result[node.name] = base if count == 1 else f"{base}_{count}"
        return result

    @staticmethod
    def _python(value: Any) -> str:
        return repr(value)

    @staticmethod
    def _as_columns(value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            if text.startswith("["):
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, list):
                        return [str(item) for item in parsed]
                except json.JSONDecodeError:
                    pass
            return [part.strip() for part in text.split(",") if part.strip()]
        return [str(value)]

    def _topological_nodes(self) -> List[Node]:
        by_name = {node.name: node for node in self.pipeline.nodes}
        indegree = {name: 0 for name in by_name}
        outgoing = defaultdict(list)
        for edge in self.pipeline.edges:
            if edge.origin not in by_name or edge.destination not in by_name:
                raise DatabricksCompileError(
                    f"Invalid edge {edge.origin!r} -> {edge.destination!r}"
                )
            outgoing[edge.origin].append(edge.destination)
            indegree[edge.destination] += 1

        ready = sorted(
            (by_name[name] for name, degree in indegree.items() if degree == 0),
            key=lambda node: (node.priority, node.name),
        )
        ordered: List[Node] = []
        while ready:
            node = ready.pop(0)
            ordered.append(node)
            for destination in outgoing[node.name]:
                indegree[destination] -= 1
                if indegree[destination] == 0:
                    ready.append(by_name[destination])
                    ready.sort(key=lambda item: (item.priority, item.name))

        if len(ordered) != len(by_name):
            cyclic = sorted(name for name, degree in indegree.items() if degree > 0)
            raise DatabricksCompileError(
                "Pipeline contains a cycle involving: " + ", ".join(cyclic)
            )
        return ordered

    def _edge_variable(self, edge: Any) -> str:
        base = self._variables[edge.origin]
        relation = getattr(edge.data_type, "value", edge.data_type)
        if relation == DataRelation.INVALID_DATA.value:
            return f"{base}__discarded"
        return base

    def _inputs(self, node: Node) -> List[str]:
        return [self._edge_variable(edge) for edge in self._incoming[node.name]]

    def _single_input(self, node: Node) -> str:
        inputs = self._inputs(node)
        if len(inputs) != 1:
            raise DatabricksCompileError(
                f"Node {node.name!r} ({node.class_name}) requires exactly one input; "
                f"found {len(inputs)}"
            )
        return inputs[0]

    def _mapping_table(self, node: Node, destination: bool = False) -> Optional[str]:
        section = self.destinations if destination else self.sources
        value = section.get(node.name)
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            table = value.get("table")
            return str(table) if table else None
        return None

    def _mapping_entry(self, node: Node, destination: bool = False) -> Dict[str, Any]:
        section = self.destinations if destination else self.sources
        value = section.get(node.name)
        return dict(value) if isinstance(value, dict) else {}

    def _transformation_entry(self, node: Node) -> Dict[str, Any]:
        value = self.transformations.get(node.name)
        return dict(value) if isinstance(value, dict) else {}

    @staticmethod
    def _option_lines(reader: str, options: Mapping[str, Any]) -> str:
        result = reader
        for key, value in options.items():
            if value is not None and value != "":
                result += f".option({key!r}, {value!r})"
        return result

    @staticmethod
    def _secret_option(reader: str, mapping: Mapping[str, Any]) -> str:
        secret = mapping.get("password_secret")
        if isinstance(secret, dict) and secret.get("scope") and secret.get("key"):
            return (
                reader
                + ".option('password', dbutils.secrets.get("
                + f"scope={secret['scope']!r}, key={secret['key']!r}))"
            )
        return reader

    def _compile_sql_input(self, node: Node) -> str:
        variable = self._variables[node.name]
        table = self._mapping_table(node)
        if table:
            body = f"{variable} = spark.table({self._python(table)})"
            if node.configuration.get("cacheTable"):
                body += f"\n{variable} = {variable}.cache()"
            return body
        query = str(node.configuration.get("query", "")).strip()
        if not query:
            return (
                f"print('WARNING: SQL input {node.name} has no query or Unity Catalog mapping')\n"
                f"{variable} = spark.range(0)"
            )
        body = f"{variable} = spark.sql(_render_parameters({self._python(query)}))"
        if node.configuration.get("cacheTable"):
            body += f"\n{variable} = {variable}.cache()"
        return body

    def _compile_file_input(self, node: Node, fmt: str) -> str:
        variable = self._variables[node.name]
        config = node.configuration
        reader = f"spark.read.format({self._python(fmt)})"
        options: Dict[str, Any] = {}
        if node.class_name == "CsvInputStep":
            options.update(delimiter=config.get("delimiter", ","), header=config.get("header", False))
        elif node.class_name == "JsonInputStep":
            options["multiLine"] = config.get("multilineEnabled", False)
        elif node.class_name == "DeltaInputStep" and config.get(
            "enableReadingOfOlderVersions"
        ):
            option = config.get("readOlderVersionBy", "versionAsOf")
            value = config.get(option)
            if value not in (None, ""):
                options[option] = value
        if config.get("pathGlobFilter"):
            options["pathGlobFilter"] = config["pathGlobFilter"]
        for key, value in options.items():
            reader += f".option({self._python(key)}, {self._python(value)})"
        paths = config.get("paths")
        if isinstance(paths, list) and paths:
            normalized = [
                item.get("path", item) if isinstance(item, dict) else item
                for item in paths
            ]
            lines = [f"{variable} = {reader}.load({normalized[0]!r})"]
            lines.extend(
                f"{variable} = {variable}.unionByName({reader}.load({path!r}), allowMissingColumns=True)"
                for path in normalized[1:]
            )
            return "\n".join(lines)
        body = f"{variable} = {reader}.load({self._python(config.get('path', ''))})"
        if node.class_name == "FileSystemInputStep":
            field = config.get("outputField", "raw")
            body += f".withColumnRenamed('value', {field!r})"
        return body

    def _compile_jdbc_input(self, node: Node) -> str:
        config = node.configuration
        mapping = self._mapping_entry(node)
        table = self._mapping_table(node)
        if table:
            return f"{self._variables[node.name]} = spark.table({table!r})"
        dbtable = config.get("dbtable", "")
        if node.class_name == "PostgresInputStep" and str(
            config.get("selectInput", "TABLE")
        ).upper() == "EXPRESSION":
            expression = config.get("selectExp", "")
            dbtable = f"({expression}) AS py2rocket_source"
        options = {
            "url": mapping.get("url", config.get("url", "")),
            "dbtable": mapping.get("dbtable", dbtable),
            "driver": mapping.get(
                "driver",
                config.get("driver", "org.postgresql.Driver"),
            ),
            "user": mapping.get("user"),
            "password": mapping.get("password"),
            "fetchsize": mapping.get("fetchsize"),
        }
        reader = self._option_lines("spark.read.format('jdbc')", options)
        reader = self._secret_option(reader, mapping)
        return f"{self._variables[node.name]} = {reader}.load()"

    def _compile_sftp_input(self, node: Node) -> str:
        config = node.configuration
        mapping = self._mapping_entry(node)
        fmt = mapping.get("format") or config.get("dataSourceClass") or "com.springml.spark.sftp"
        options = {
            "host": mapping.get("host", config.get("host")),
            "port": mapping.get("port", config.get("port")),
            "username": mapping.get("username", config.get("username")),
            "password": mapping.get("password", config.get("password")),
            "fileType": mapping.get("fileType", config.get("fileType")),
        }
        options.update(_literal_options(config.get("inputOptions")))
        options.update(mapping.get("options", {}))
        reader = self._option_lines(f"spark.read.format({fmt!r})", options)
        reader = self._secret_option(reader, mapping)
        path = mapping.get("path", config.get("path", ""))
        return f"{self._variables[node.name]} = {reader}.load({path!r})"

    def _compile_custom_input(self, node: Node) -> str:
        config = node.configuration
        mapping = self._mapping_entry(node)
        fmt = mapping.get("format") or config.get("customLiteClassType")
        if not fmt:
            fmt = "py2rocket.custom"
        options = _literal_options(config.get("inputOptions"))
        options.update(mapping.get("options", {}))
        reader = self._option_lines(f"spark.read.format({fmt!r})", options)
        path = mapping.get("path")
        suffix = f".load({path!r})" if path else ".load()"
        return f"{self._variables[node.name]} = {reader}{suffix}"

    def _compile_test_input(self, node: Node) -> str:
        config = node.configuration
        variable = self._variables[node.name]
        count = int(config.get("numEvents") or config.get("maxNumber") or 10)
        event = str(config.get("event", ""))
        field = str(config.get("outputField", "raw"))
        event_type = str(config.get("eventType", "STRING")).upper()
        if event_type == "JSON":
            return (
                f"{variable} = spark.read.json("
                f"spark.sparkContext.parallelize([{event!r}] * {count}))"
            )
        if event_type in {"NUMBER", "INTEGER", "LONG"} and not event:
            return f"{variable} = spark.range({count}).withColumnRenamed('id', {field!r})"
        return (
            f"{variable} = spark.range({count}).select("
            f"F.lit({event!r}).alias({field!r}))"
        )

    def _compile_pyspark(self, node: Node) -> str:
        variable = self._variables[node.name]
        inputs = self._inputs(node)
        code = str(node.configuration.get("pythonCode", "")).strip()
        prefix = [f"dfs = [{', '.join(inputs)}]"]
        if inputs:
            prefix.append("df = dfs[0]")
        if not code:
            return f"{variable} = {inputs[0] if inputs else 'spark.range(0)'}"
        # The common Rocket form is a single expression such as df.filter(...).
        if "\n" not in code and not re.match(
            r"^(from |import |for |while |if |try:|with |def |class |result\s*=|df\s*=)",
            code,
        ):
            prefix.append(f"{variable} = ({code})")
        else:
            prefix.append(code)
            prefix.append(
                f"{variable} = locals().get('result', locals().get('output', df))"
            )
        return "\n".join(prefix)

    def _compile_transform(self, node: Node) -> str:
        cls = node.class_name
        config = node.configuration
        variable = self._variables[node.name]

        if cls == "PySparkTransformerStep":
            return self._compile_pyspark(node)
        if cls == "CustomLiteXDTransformStep":
            inputs = self._inputs(node)
            mapping = self._transformation_entry(node)
            adapter = mapping.get("adapter", node.configuration.get("customLiteClassType", cls))
            return (
                f"{variable} = _run_adapter({adapter!r}, {node.name!r}, "
                f"{node.configuration!r}, [{', '.join(inputs)}])"
            )
        if cls == "MlModelTransformStep":
            source = self._single_input(node)
            mapping = self._transformation_entry(node)
            model_uri = mapping.get("model_uri") or node.configuration.get("MlModelAux")
            result_type = mapping.get(
                "result_type", node.configuration.get("predictionColumnType", "string")
            )
            result_type = _spark_type_name(result_type)
            prediction = node.configuration.get("predictionColumnName") or "prediction"
            feature_columns = mapping.get("feature_columns")
            args = (
                ", ".join(f"F.col({column!r})" for column in feature_columns)
                if isinstance(feature_columns, list) and feature_columns
                else "F.struct(*[F.col(_column) for _column in " + source + ".columns])"
            )
            return "\n".join(
                [
                    "import mlflow",
                    f"_model_udf = mlflow.pyfunc.spark_udf(spark, model_uri={model_uri!r}, result_type={result_type!r})",
                    f"{variable} = {source}.withColumn({prediction!r}, _model_udf({args}))",
                ]
            )
        if cls == "TriggerTransformStep":
            inputs = self._inputs(node)
            if not inputs:
                return f"{variable} = spark.sql(_render_parameters({node.configuration.get('sql', '')!r}))"
            lines = [
                f"{item}.createOrReplaceTempView({item!r})" for item in inputs
            ]
            sql = str(node.configuration.get("sql", "")).strip()
            replace = bool(node.configuration.get("replaceWithInputDataframe"))
            if replace or not sql:
                lines.append(f"{variable} = {inputs[0]}")
            else:
                if re.match(r"(?is)^\s*select\b", sql) and not re.search(
                    r"(?is)\bfrom\b", sql
                ):
                    if re.search(r"(?is)\bwhere\b", sql):
                        sql = re.sub(
                            r"(?is)\bwhere\b",
                            f"FROM {inputs[0]} WHERE",
                            sql,
                            count=1,
                        )
                    else:
                        sql = f"{sql} FROM {inputs[0]}"
                lines.append(
                    f"{variable} = spark.sql(_render_parameters({sql!r}))"
                )
            discarded = str(node.configuration.get("discardConditions", "")).strip()
            if discarded:
                lines.append(f"_discard_condition = _render_parameters({discarded!r})")
                lines.append(
                    f"{variable}__discarded = {inputs[0]}.filter(_discard_condition)"
                )
            else:
                lines.append(f"{variable}__discarded = {inputs[0]}.limit(0)")
            return "\n".join(lines)
        if cls == "UnionTransformStep":
            inputs = self._inputs(node)
            if not inputs:
                raise DatabricksCompileError(f"Union node {node.name!r} has no inputs")
            lines = [f"{variable} = {inputs[0]}"]
            lines.extend(f"{variable} = {variable}.unionByName({item})" for item in inputs[1:])
            return "\n".join(lines)

        source = self._single_input(node)
        if cls == "AddColumnsTransformStep":
            expressions = config.get("addColumnExpressionList") or config.get("columns") or []
            lines = [f"{variable} = {source}"]
            for item in expressions:
                field = item.get("field", item.get("name"))
                expression = item.get("query", item.get("expression"))
                if not field or expression is None:
                    raise DatabricksCompileError(f"Invalid AddColumns item in {node.name!r}: {item!r}")
                lines.append(
                    f"{variable} = {variable}.withColumn({self._python(field)}, F.expr({self._python(expression)}))"
                )
            return "\n".join(lines)
        if cls == "DropColumnsTransformStep":
            fields = config.get("schema.fields", [])
            columns = [item.get("name") for item in fields if isinstance(item, dict) and item.get("name")]
            return f"{variable} = {source}.drop(*{self._python(columns)})"
        if cls == "RenameColumnTransformationStep":
            lines = [f"{variable} = {source}"]
            for item in config.get("columns", []):
                if isinstance(item, dict) and item.get("name") and item.get("alias"):
                    lines.append(
                        f"{variable} = {variable}.withColumnRenamed({self._python(item['name'])}, {self._python(item['alias'])})"
                    )
            return "\n".join(lines)
        if cls == "SelectTransformStep":
            select_exp = config.get("selectExp")
            columns = self._as_columns(config.get("columns"))
            if select_exp:
                return f"{variable} = {source}.selectExpr({self._python(select_exp)})"
            return f"{variable} = {source}.select(*{self._python(columns)})"
        if cls == "DistinctTransformStep":
            return f"{variable} = {source}.distinct()"
        if cls == "DropDuplicatesTransformStep":
            columns = self._as_columns(config.get("columns"))
            partition = (
                self._python(columns)
                if columns
                else f"[F.col(_column) for _column in {source}.columns]"
            )
            return "\n".join(
                [
                    f"_dedup_window = Window.partitionBy(*{partition}).orderBy(F.lit(1))",
                    f"_dedup_ranked = {source}.withColumn('__py2rocket_row_number', F.row_number().over(_dedup_window))",
                    f"{variable} = _dedup_ranked.filter(F.col('__py2rocket_row_number') == 1).drop('__py2rocket_row_number')",
                    f"{variable}__discarded = _dedup_ranked.filter(F.col('__py2rocket_row_number') > 1).drop('__py2rocket_row_number')",
                ]
            )
        if cls == "FilterTransformStep":
            expression = str(config.get("filterExp", ""))
            return "\n".join(
                [
                    f"_condition = _render_parameters({self._python(expression)})",
                    f"{variable} = {source}.filter(_condition)",
                    f"{variable}__discarded = {source}.filter((~F.expr(_condition)) | F.expr(_condition).isNull())",
                ]
            )
        if cls == "CoalesceTransformStep":
            count = int(config.get("partitions") or 1)
            return f"{variable} = {source}.coalesce({count})"
        if cls == "RepartitionTransformStep":
            count = config.get("partitions")
            columns = self._as_columns(config.get("columns"))
            has_count = count is not None and str(count).strip() != ""
            args = ([str(int(count))] if has_count else []) + [
                f"F.col({self._python(column)})" for column in columns
            ]
            if not args:
                return f"{variable} = {source}"
            return f"{variable} = {source}.repartition({', '.join(args)})"
        if cls == "PersistTransformStep":
            return f"{variable} = {source}.persist()"
        if cls == "ByPassStep":
            return f"{variable} = {source}"
        raise DatabricksCompileError(
            f"Unsupported Databricks transformation {node.name!r}: {cls}"
        )

    @staticmethod
    def _save_mode(value: Any) -> str:
        normalized = str(value or "overwrite").strip().lower()
        return {"errorifexists": "error", "statement": "append"}.get(normalized, normalized)

    def _compile_output(self, node: Node) -> str:
        cls = node.class_name
        config = node.configuration
        inputs = self._inputs(node)
        if cls == "RunWorkflowOutputStep":
            mapping = self._mapping_entry(node, destination=True)
            notebook = mapping.get("notebook") or config.get("workflowId")
            arguments = mapping.get("arguments", {})
            timeout = int(mapping.get("timeout_seconds", 0))
            lines = [
                f"_run_arguments = {{**PARAMETERS, **{arguments!r}}}",
                f"_run_result = dbutils.notebook.run({notebook!r}, {timeout}, _run_arguments)",
                f"print({node.name!r}, _run_result)",
            ]
            return "\n".join(lines)
        if not inputs:
            raise DatabricksCompileError(f"Output node {node.name!r} has no inputs")
        source = inputs[0] + "".join(
            f".unionByName({item}, allowMissingColumns=True)" for item in inputs[1:]
        )
        table = self._mapping_table(node, destination=True)

        if cls == "PrintOutputStep":
            lines: List[str] = []
            if config.get("printSchema"):
                lines.append(f"{source}.printSchema()")
            if config.get("printMetadata", True):
                lines.append(f"print({self._python(node.name)}, 'rows=', {source}.count(), 'columns=', len({source}.columns))")
            if config.get("printData"):
                lines.append(f"display({source})")
            return "\n".join(lines or [f"display({source})"])
        if cls == "PySparkOutputStep":
            code = str(config.get("pythonCode", "")).strip()
            return f"df = {source}\ndfs = [{', '.join(inputs)}]\n{code}"
        if cls in {"JdbcOutputStep", "PostgresOutputStep"}:
            mapping = self._mapping_entry(node, destination=True)
            options = {
                "url": mapping.get("url", config.get("url", "")),
                "dbtable": mapping.get("dbtable", config.get("dbtable", "")),
                "driver": mapping.get(
                    "driver",
                    config.get("driver", "org.postgresql.Driver"),
                ),
                "user": mapping.get("user"),
                "password": mapping.get("password"),
                "batchsize": mapping.get("batchsize", config.get("batchsize")),
                "isolationLevel": config.get("isolationLevel"),
            }
            options.update(mapping.get("options", {}))
            writer = self._option_lines(
                f"{source}.write.mode({self._save_mode(mapping.get('mode', 'append'))!r}).format('jdbc')",
                options,
            )
            writer = self._secret_option(writer, mapping)
            return f"{writer}.save()"
        if cls == "SFTPOutputStep":
            mapping = self._mapping_entry(node, destination=True)
            fmt = mapping.get("format") or config.get("dataSourceClass") or "com.springml.spark.sftp"
            options = {
                "host": mapping.get("host", config.get("host")),
                "port": mapping.get("port", config.get("port")),
                "username": mapping.get(
                    "username", config.get("sftpServerUsername")
                ),
                "password": mapping.get("password", config.get("password")),
                "fileType": mapping.get("fileType", config.get("fileType")),
            }
            options.update(_literal_options(config.get("saveOptions")))
            options.update(mapping.get("options", {}))
            writer = self._option_lines(
                f"{source}.write.mode({self._save_mode(mapping.get('mode', 'overwrite'))!r}).format({fmt!r})",
                options,
            )
            writer = self._secret_option(writer, mapping)
            path = mapping.get("path", config.get("path", ""))
            return f"{writer}.save({path!r})"
        if cls == "CustomLiteXDOutputStep":
            mapping = self._mapping_entry(node, destination=True)
            fmt = mapping.get("format") or config.get("customLiteClassType") or "py2rocket.custom"
            options = _literal_options(config.get("outputOptions"))
            options.update(mapping.get("options", {}))
            writer = self._option_lines(
                f"{source}.write.mode({self._save_mode(mapping.get('mode', 'append'))!r}).format({fmt!r})",
                options,
            )
            path = mapping.get("path")
            return f"{writer}.save({path!r})" if path else f"{writer}.save()"
        if table:
            mode = self._save_mode(config.get("saveMode", "overwrite"))
            return f"{source}.write.mode({self._python(mode)}).saveAsTable({self._python(table)})"
        formats = {
            "DeltaOutputStep": "delta",
            "ParquetOutputStep": "parquet",
            "JsonOutputStep": "json",
            "CsvOutputStep": "csv",
            "TextOutputStep": "text",
        }
        if cls in formats:
            mode = self._save_mode(config.get("saveMode", "overwrite"))
            writer = f"{source}.write.mode({self._python(mode)}).format({self._python(formats[cls])})"
            if cls == "CsvOutputStep":
                writer += f".option('header', {self._python(config.get('header', False))})"
                writer += f".option('delimiter', {self._python(config.get('delimiter', ','))})"
            return f"{writer}.save({self._python(config.get('path', ''))})"
        raise DatabricksCompileError(f"Unsupported Databricks output {node.name!r}: {cls}")

    def _compile_node(self, node: Node) -> str:
        input_formats = {
            "ParquetInputStep": "parquet",
            "DeltaInputStep": "delta",
            "JsonInputStep": "json",
            "CsvInputStep": "csv",
            "FileSystemInputStep": "text",
        }
        if node.class_name == "SQLInputStep":
            body = self._compile_sql_input(node)
        elif node.class_name in {"JdbcInputStep", "PostgresInputStep"}:
            body = self._compile_jdbc_input(node)
        elif node.class_name == "SFTPInputStep":
            body = self._compile_sftp_input(node)
        elif node.class_name == "CustomLiteXDInputStep":
            body = self._compile_custom_input(node)
        elif node.class_name == "TestInputStep":
            body = self._compile_test_input(node)
        elif node.class_name in input_formats:
            body = self._compile_file_input(node, input_formats[node.class_name])
        elif node.class_name == "PySparkInputStep":
            body = self._compile_pyspark(node)
        elif node.step_type.value == "Transformation":
            body = self._compile_transform(node)
        elif node.step_type.value == "Output":
            body = self._compile_output(node)
        else:
            raise DatabricksCompileError(
                f"Unsupported Databricks input {node.name!r}: {node.class_name}"
            )
        description = f"\n# {node.description}" if node.description else ""
        return f"# Node: {node.name} [{node.class_name}]{description}\n{body}"

    def compile(self) -> str:
        cells = [
            "# Databricks notebook source\n"
            "# Generated by py2rocket. Edit the DSL source, not this notebook."
        ]
        parameters = self.pipeline.parameters or {}
        lines = ["# Pipeline parameters", "PARAMETERS = {}"]
        for name, default in parameters.items():
            lines.append(f"dbutils.widgets.text({self._python(name)}, {self._python(str(default))})")
            lines.append(f"PARAMETERS[{self._python(name)}] = dbutils.widgets.get({self._python(name)})")
            lines.append(f"{self._identifier(name)} = PARAMETERS[{self._python(name)}]")
        lines.extend(
            [
                "",
                "def _render_parameters(value):",
                "    for _name, _value in PARAMETERS.items():",
                "        value = value.replace('{{{' + _name + '}}}', _value)",
                "        value = value.replace('{{' + _name + '}}', _value)",
                "    return value",
                "",
                "PY2ROCKET_ADAPTERS = globals().get('PY2ROCKET_ADAPTERS', {})",
                "",
                "def _run_adapter(adapter, node_name, configuration, dfs):",
                "    implementation = PY2ROCKET_ADAPTERS.get(adapter)",
                "    if implementation is not None:",
                "        return implementation(spark, dfs, configuration)",
                "    print(f'WARNING: adapter {adapter!r} for {node_name!r} is not registered; using passthrough')",
                "    return dfs[0] if dfs else spark.range(0)",
            ]
        )
        cells.append("\n".join(lines))
        cells.append(
            "from pyspark.sql import functions as F\n"
            "from pyspark.sql.window import Window"
        )
        cells.extend(self._compile_node(node) for node in self._topological_nodes())
        return self.CELL_SEPARATOR.join(cells) + "\n"

    def save(self, output_path: str) -> str:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.compile(), encoding="utf-8")
        return str(path)
