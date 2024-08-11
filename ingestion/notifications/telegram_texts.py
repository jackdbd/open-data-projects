import datetime
import dlt
from typing import Any, Optional
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.specs import RunConfiguration
from dlt.common.json import json
from dlt.common.pipeline import LoadInfo
from dlt.pipeline import Pipeline
from dlt.pipeline.exceptions import PipelineStepFailed

# https://core.telegram.org/bots/api#sendmessage
MAX_TEXT_LENGHT = 4096
DEFAULT_APP_NAME = "My App"

ERROR_EMOJI = "❌"
GEAR_EMOJI = "⚙️"
INFO_EMOJI = "ℹ️"
WARNING_EMOJI = "⚠️"


def pipeline_step_failed(
    exception: PipelineStepFailed, app_name: Optional[str] = DEFAULT_APP_NAME
):
    pipeline_name = exception.pipeline.pipeline_name
    header = f"{ERROR_EMOJI} <b>dlt pipeline <code>{pipeline_name}</code> failed at step <code>{exception.step}</code></b>"
    footer = f"<i>Sent by <code>{app_name}</code></i>"

    arr = [header]

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
    arr.append(footer)

    return "".join(arr)


def schema_update(
    pipeline_name: str,
    table_name: str,
    column_name: str,
    app_name: Optional[str] = DEFAULT_APP_NAME,
):
    header = f"{WARNING_EMOJI} <b>Schema update in dlt pipeline <code>{pipeline_name}</code></b>"
    footer = f"<i>Sent by <code>{app_name}</code></i>"

    arr = [
        header,
        "\n\n",
        f"Table: <code>{table_name}</code>",
        "\n",
        f"Column: <code>{column_name}</code>",
        "\n\n",
        footer,
    ]

    return "".join(arr)


def table_schema_update(
    table_name: str, table: Any, app_name: Optional[str] = DEFAULT_APP_NAME
):
    header = f"{WARNING_EMOJI} <b>Schema update in table <code>{table_name}</code></b>"
    footer = f"<i>Sent by <code>{app_name}</code></i>"

    arr = [header]

    arr.append("\n\n<b>Columns updated:</b>")
    for column_name, column in table["columns"].items():
        arr.append(f"\n<code>{column_name}</code>")

    arr.append(f"\n\n{footer}")
    return "".join(arr)


def runtime_configuration(
    pipeline: Pipeline, app_name: Optional[str] = DEFAULT_APP_NAME
):
    # config: RunConfiguration = pipeline.runtime_config
    header = f"{GEAR_EMOJI} <b>Configuration for dlt pipeline <code>{pipeline.pipeline_name}</code></b>"
    footer = f"<i>Sent by <code>{app_name}</code></i>"

    arr = [header]

    arr.append("\n\n<b>dlt runtime:</b>")
    for k, v in pipeline.runtime_config.items():
        arr.append(f"\n{k}: <code>{v}</code>")

    arr.append(f"\n\n{footer}")
    return "".join(arr)


def config_field_missing_exception(
    exception: ConfigFieldMissingException, app_name: Optional[str] = DEFAULT_APP_NAME
):
    header = f"{ERROR_EMOJI} <b>Missing configuration in <code>{exception.spec_name}</code></b>"
    footer = f"\n\n<i>Sent by <code>{app_name}</code></i>"

    # This exception contains a lot of information and it probably exceeds the
    # limit of character length allowed by the Telegram Bot API.
    exception_str = f"\n\n<pre><code>{exception}</code></pre>"

    missing = [f"<code>{s}</code>" for s in exception.fields]
    missing_str = f"\n\n<b>Fields missing</b>: {', '.join(missing)}"

    if (
        len(header) + len(missing_str) + len(exception_str) + len(footer)
        <= MAX_TEXT_LENGHT
    ):
        return "".join([header, missing_str, exception_str, footer])
    else:
        return "".join([header, missing_str, footer])


def load_info(load_info: LoadInfo, app_name: Optional[str] = DEFAULT_APP_NAME):
    pipeline_name = load_info.pipeline.pipeline_name
    header = (
        f"{INFO_EMOJI} <b>Load info for dlt pipeline <code>{pipeline_name}</code></b>"
    )
    footer = f"<i>Sent by <code>{app_name}</code></i>"

    arr = [header]

    d = load_info.asdict()
    arr.append(
        f"\n\n<pre><code>{json.dumps(d, pretty=True, sort_keys=True)}</code></pre>"
    )

    arr.append(f"\n\n{footer}")

    text = "".join(arr)
    if len(text) <= MAX_TEXT_LENGHT:
        return text
    else:
        loads_ids = d["loads_ids"]
        return "".join(
            [
                header,
                "\n\n",
                f"<b>Loads IDs</b>\n<pre><code>{loads_ids}</code></pre>",
                "\n\n",
                footer,
            ]
        )


if __name__ == "__main__":
    app_name = "My Awesome App"
    pipeline_name = "my_pipeline"

    now = datetime.datetime.now()
    five_minutes_ago = now - datetime.timedelta(seconds=300)

    pipeline = dlt.pipeline(pipeline_name=pipeline_name)
    load_id = "12345"
    step = "extract"  # one of "sync", "extract", "normalize", "load"
    step_info = None
    # step_info = {"metrics": {"started_at": five_minutes_ago, "finished_at": now}}
    print("BEGIN text pipeline_step_failed")
    print(
        pipeline_step_failed(
            PipelineStepFailed(
                pipeline,
                exception=Exception("I am the original cause of the failure"),
                step=step,
                step_info=step_info,
                load_id=load_id,
            )
        )
    )
    print("END text pipeline_step_failed")

    table_name = "my_table"
    column_name = "my_column"
    print("\n\nBEGIN text schema_update")
    print(
        schema_update(
            app_name=app_name,
            pipeline_name=pipeline_name,
            table_name=table_name,
            column_name=column_name,
        )
    )
    print("END text schema_update")

    print("\n\nBEGIN text table_schema_update")
    print(
        table_schema_update(
            app_name=app_name,
            table_name=table_name,
            table={"columns": {"foo": "VARCHAR", "bar": "INTEGER"}},
        )
    )
    print("END text table_schema_update")
