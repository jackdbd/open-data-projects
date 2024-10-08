import datetime
from typing import Any, Optional, Sequence

import dlt
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.specs import RunConfiguration
from dlt.common.json import json
from dlt.common.pipeline import LoadInfo
from dlt.helpers.dbt import DBTPackageRunner
from dlt.helpers.dbt.exceptions import DBTNodeResult
from dlt.pipeline import Pipeline
from dlt.pipeline.exceptions import PipelineStepFailed

# https://core.telegram.org/bots/api#sendmessage
MAX_TEXT_LENGHT = 4096
DEFAULT_APP_NAME = "My App"

DATABASE_EMOJI = "⛁"
ERROR_EMOJI = "❌"
GEAR_EMOJI = "⚙️"
INFO_EMOJI = "ℹ️"
SUCCESS_EMOJI = "✅"
TEST_EMOJI = "🧪"
TIP_EMOJI = "💡"
WARNING_EMOJI = "⚠️"


def footer(app_name: Optional[str] = DEFAULT_APP_NAME):
    return f"<i>Sent by <code>{app_name}</code></i>."


def config_field_missing_exception(
    exception: ConfigFieldMissingException, app_name: Optional[str] = DEFAULT_APP_NAME
):
    header = f"{ERROR_EMOJI} <b>Missing configuration in <code>{exception.spec_name}</code></b>"
    footer_ = footer(app_name)

    # This exception contains a lot of information and it probably exceeds the
    # limit of character length allowed by the Telegram Bot API.
    exception_str = f"\n\n<pre><code>{exception}</code></pre>"

    missing = [f"<code>{s}</code>" for s in exception.fields]
    missing_str = f"\n\n<b>Fields missing</b>: {', '.join(missing)}"

    if (
        len(header) + len(missing_str) + len(exception_str) + len(footer_)
        <= MAX_TEXT_LENGHT
    ):
        return "".join([header, missing_str, exception_str, footer_])
    else:
        return "".join([header, missing_str, footer_])


def dbt_info(dbt: DBTPackageRunner):
    d = {
        # "config": dbt.config, # not too useful IMHO
        "source_dataset_name": dbt.source_dataset_name,
        "package_path": dbt.package_path,
        "working_dir": dbt.working_dir,
    }

    arr = [
        "Info on the dbt environment.",
        f"\n<pre><code>{json.dumps(d, pretty=True, sort_keys=True)}</code></pre>",
    ]
    return "".join(arr)


def dbt_model_materialized(
    dbt: DBTPackageRunner, model: Any, app_name: Optional[str] = DEFAULT_APP_NAME
):
    header = f"{DATABASE_EMOJI} <b>Model materialized</b>"

    arr = [
        header,
        "\n\n",
        f"dbt model <code>{model.model_name}</code> materialized in {model.time:.2f} seconds with status <code>{model.status}</code>.",
    ]

    arr.append(f"\n\nMessage: {model.message}")
    arr.append(f"\n\nSource dataset name: <code>{dbt.source_dataset_name}</code>")
    arr.append(f"\n\nPackage path: <code>{dbt.package_path}</code>")

    arr.append(f"\n\n{footer(app_name)}")
    return "".join(arr)


def dbt_models_recap(
    dbt_node_results: Sequence[DBTNodeResult],
    dbt: DBTPackageRunner = None,
    app_name: Optional[str] = DEFAULT_APP_NAME,
):
    header = f"{DATABASE_EMOJI} <b>dbt models</b>"
    arr = [header, "\n\n", f"{len(dbt_node_results)} dbt models materialized.", "\n"]

    for res in dbt_node_results:
        emoji = SUCCESS_EMOJI if res.status == "success" else ERROR_EMOJI
        desc = f"<code>{res.model_name}</code> materialized in {res.time:.2f} seconds."
        arr.append(f"\n{emoji} {desc}")

    if dbt:
        arr.append(f"\n\n{dbt_info(dbt)}")
    arr.append(f"\n\n{footer(app_name)}")
    return "".join(arr)


def dbt_tests_recap(
    dbt_node_results: Sequence[DBTNodeResult],
    dbt: DBTPackageRunner = None,
    app_name: Optional[str] = DEFAULT_APP_NAME,
):
    header = f"{TEST_EMOJI} <b>dbt tests</b>"
    arr = [
        header,
        "\n\n",
        f"{len(dbt_node_results)} tests on dbt sources and models.",
        "\n",
    ]

    for res in dbt_node_results:
        emoji = SUCCESS_EMOJI if res.status == "pass" else ERROR_EMOJI
        arr.append(f"\n{emoji} {res.model_name}")

    if dbt:
        arr.append(f"\n\n{dbt_info(dbt)}")
    arr.append(f"\n\n{footer(app_name)}")
    return "".join(arr)


def generic_exception(
    exception: Exception,
    app_name: Optional[str] = DEFAULT_APP_NAME,
    tip: Optional[str] = None,
):
    header = f"{ERROR_EMOJI} <b>Exception</b>"
    arr = [header]

    arr.append(f"\n\n<pre><code>{exception}</code></pre>")
    if tip:
        arr.append(f"\n\n{TIP_EMOJI} <b>TIP</b>\n{tip}")

    arr.append(f"\n\n{footer(app_name)}")
    return "".join(arr)


def load_info(load_info: LoadInfo, app_name: Optional[str] = DEFAULT_APP_NAME):
    pipeline_name = load_info.pipeline.pipeline_name
    destination_name = load_info.destination_name
    dataset_name = load_info.dataset_name
    header = f"{DATABASE_EMOJI} <b>Load info</b>"

    arr = [
        header,
        "\n\n",
        f"dlt pipeline <code>{pipeline_name}</code> for destination <code>{destination_name}</code>, dataset <code>{dataset_name}</code>, ran to completion.",
    ]

    d = load_info.asdict()
    for i, metric in enumerate(d["metrics"]):
        load_id = metric["load_id"]
        arr.append(f"\n\n<b>Load {i+1}</b>")
        arr.append(
            f"\n<pre><code>{json.dumps(metric, pretty=True, sort_keys=True)}</code></pre>"
        )
        arr.append(f"\nRun this command for more info:")
        arr.append(
            f"\n<pre><code>dlt pipeline {pipeline_name} load-package {load_id}</code></pre>"
        )

    arr.append(f"\n\n{footer(app_name)}")
    return "".join(arr)


def pipeline_step_failed(
    exception: PipelineStepFailed, app_name: Optional[str] = DEFAULT_APP_NAME
):
    pipeline_name = exception.pipeline.pipeline_name
    header = f"{ERROR_EMOJI} <b>Pipeline failed</b>"

    arr = [
        header,
        "\n\n",
        f"dlt pipeline <code>{pipeline_name}</code> failed at step <code>{exception.step}</code>.",
    ]

    if exception.exception:
        arr.append("\n\n")
        arr.append("<b>Exception</b>")
        arr.append("\n")
        arr.append(f"<pre><code>{exception.exception}</code></pre>")

    if exception.step_info:
        arr.append("\n\n")
        arr.append("<b>Step info</b>")
        arr.append("\n")
        arr.append(f"<pre><code>{exception.step_info}</code></pre>")

    if exception.load_id:
        arr.append("\n\n")
        arr.append("<b>Load ID</b>")
        arr.append("\n")
        arr.append(f"<pre><code>{exception.load_id}</code></pre>")

    arr.append("\n\n")
    arr.append(footer(app_name))

    return "".join(arr)


def runtime_configuration(
    pipeline: Pipeline, app_name: Optional[str] = DEFAULT_APP_NAME
):
    header = f"{GEAR_EMOJI} <b>Run configuration</b>"
    cfg: RunConfiguration = pipeline.runtime_config

    arr = [
        header,
        "\n\n<b>dlt runtime</b>",
        f"\n<pre><code>{json.dumps(cfg, pretty=True, sort_keys=True)}</code></pre>",
        f"\n\n{footer(app_name)}",
    ]

    return "".join(arr)


def table_schema_update(
    table_name: str, table: Any, app_name: Optional[str] = DEFAULT_APP_NAME
):
    header = f"{WARNING_EMOJI} <b>Schema update</b>"
    table_info = {k: v for k, v in table.items() if k != "columns" and k != "name"}

    arr = [
        header,
        "\n\n",
        f"Table <code>{table_name}</code> changed its schema.",
        f"\n\n<pre><code>{json.dumps(table_info, pretty=True, sort_keys=True)}</code></pre>",
    ]

    arr.append("\n\n<b>Columns</b>")
    for column_name, column in table["columns"].items():
        column_info = {k: v for k, v in column.items() if k != "name"}
        arr.append(f"\n\n{column_name}")
        arr.append(
            f"\n<pre><code>{json.dumps(column_info, pretty=True, sort_keys=True)}</code></pre>"
        )

    arr.append(f"\n\n{footer(app_name)}")
    return "".join(arr)


if __name__ == "__main__":
    import os
    import sys

    # Add the parent directory to the system path so that I can import Python code from sibling directories.
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from common import APP_NAME, TEST_DLT_PIPELINE_NAME

    app_name = APP_NAME
    pipeline_name = TEST_DLT_PIPELINE_NAME

    now = datetime.datetime.now()
    ten_seconds_ago = now - datetime.timedelta(seconds=10)
    two_minutes_ago = now - datetime.timedelta(seconds=120)
    five_minutes_ago = now - datetime.timedelta(seconds=300)

    pipeline = dlt.pipeline(pipeline_name=pipeline_name)
    load_id = "12345"
    load_id_second = "67890"
    step = "extract"  # one of "sync", "extract", "normalize", "load"
    step_info = None
    # step_info = {"metrics": {"started_at": five_minutes_ago, "finished_at": now}}
    print("BEGIN text pipeline_step_failed")
    print(
        pipeline_step_failed(
            exception=PipelineStepFailed(
                pipeline,
                exception=Exception("I am the original cause of the failure"),
                step=step,
                step_info=step_info,
                load_id=load_id,
            ),
            app_name=app_name,
        )
    )
    print("END text pipeline_step_failed")

    table_name = "my_table"
    columns = {"foo": {"data_type": "VARCHAR"}, "bar": {"data_type": "INTEGER"}}
    print("\n\nBEGIN text table_schema_update")
    print(
        table_schema_update(
            app_name=app_name,
            table_name=table_name,
            table={"columns": columns},
        )
    )
    print("END text table_schema_update")

    destination_name = "duckdb"
    dataset_name = "landing_zone"

    class FakeDbt:
        def __init__(self):
            self.source_dataset_name = dataset_name
            self.package_path = "/some/path/to/my/dbt/package"

    class FakeDbtModel:
        def __init__(self):
            self.model_name = "my_dbt_model"
            self.status = "materialized"
            self.message = "OK"
            self.time = 7.123

    print("\n\nBEGIN text dbt_model_materialized")
    print(
        dbt_model_materialized(app_name=app_name, dbt=FakeDbt(), model=FakeDbtModel())
    )
    print("END text dbt_model_materialized")

    class FakeLoadInfo:
        def __init__(self):
            self.dataset_name = dataset_name
            self.destination_name = destination_name
            self.loads_ids = [load_id, load_id_second]
            self.pipeline = pipeline

        def asdict(self):
            return {
                "dataset_name": dataset_name,
                "destination_name": destination_name,
                "loads_ids": [load_id],
                "pipeline": {"pipeline_name": pipeline_name},
                "metrics": [
                    {
                        "load_id": load_id,
                        "started_at": five_minutes_ago,
                        "finished_at": now,
                    },
                    {
                        "load_id": load_id_second,
                        "started_at": two_minutes_ago,
                        "finished_at": ten_seconds_ago,
                    },
                ],
            }

    print("\n\nBEGIN text load_info")
    print(load_info(app_name=app_name, load_info=FakeLoadInfo()))
    print("END text load_info")
