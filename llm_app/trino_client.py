import os

import pandas as pd
import trino
from dotenv import load_dotenv
from trino.exceptions import TrinoConnectionError
from trino.exceptions import TrinoUserError


load_dotenv()


def get_connection_settings(host: str) -> dict[str, str | int]:
    return {
        "host": host,
        "port": int(os.getenv("TRINO_PORT", "8080")),
        "user": os.getenv("TRINO_USER", "tead_llm"),
        "catalog": os.getenv("TRINO_CATALOG", "iceberg"),
        "schema": os.getenv("TRINO_SCHEMA", "gold"),
    }


def get_trino_hosts() -> list[str]:
    configured_host = os.getenv("TRINO_HOST", "localhost")
    hosts = [configured_host]

    # This fallback helps when the add-on is run inside a Docker container on the project network.
    if configured_host != "tead-trino":
        hosts.append("tead-trino")

    return hosts


def run_select_query(sql: str) -> pd.DataFrame:
    last_error = None

    for host in get_trino_hosts():
        connection = None
        cursor = None

        try:
            connection = trino.dbapi.connect(**get_connection_settings(host))
            cursor = connection.cursor()
            cursor.execute(sql)

            rows = cursor.fetchall()
            columns = [item[0] for item in cursor.description]

            return pd.DataFrame(rows, columns=columns)
        except TrinoConnectionError as exc:
            last_error = exc
        except TrinoUserError as exc:
            raise ValueError(f"Trino could not run the query: {exc.message}") from exc
        finally:
            if cursor is not None:
                cursor.close()

            if connection is not None:
                connection.close()

    if last_error is not None:
        raise ValueError(
            "Could not connect to Trino. Check TRINO_HOST and make sure the Trino service is reachable."
        ) from last_error

    raise ValueError("Could not connect to Trino.")
