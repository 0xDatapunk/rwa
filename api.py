from databricks import sql
import os
from fastapi import FastAPI

app = FastAPI()


@app.get("/top_tokens/{period}")
def read_item(item_id: int, q: Union[str, None] = None):
    with sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                    http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                    access_token    = os.getenv("DATABRICKS_TOKEN")) as connection:

        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM default.top_tokens where period={period}")
            result = cursor.fetchall()
    return result
