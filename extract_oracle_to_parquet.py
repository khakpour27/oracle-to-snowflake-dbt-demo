import os, pandas as pd, datetime as dt, pathlib
from dotenv import load_dotenv
import oracledb
from azure.storage.blob import BlobServiceClient

load_dotenv()

ORA_USER = os.getenv("ORA_USER","fonds")
ORA_PASSWORD = os.getenv("ORA_PASSWORD","F0ndsPass!")
ORA_HOST = os.getenv("ORA_HOST","localhost")
ORA_PORT = int(os.getenv("ORA_PORT","1521"))
ORA_SERVICE = os.getenv("ORA_SERVICE","XEPDB1")

AZURE_CONN_STR = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER","sfraw")
PREFIX = "oracle/fonds/fond_transactions"

wm_file = pathlib.Path(__file__).parent / ".wm_fond_transactions"
last_wm = "1970-01-01 00:00:00"
if wm_file.exists():
    last_wm = wm_file.read_text().strip()

dsn = oracledb.makedsn(ORA_HOST, ORA_PORT, service_name=ORA_SERVICE)
conn = oracledb.connect(user=ORA_USER, password=ORA_PASSWORD, dsn=dsn, mode=oracledb.AUTH_MODE_DEFAULT)
cur = conn.cursor()

sql = '''
SELECT TX_ID, ACCOUNT_ID, FUND_ID, TX_TYPE, QUANTITY, NAV, AMOUNT, TX_TS
FROM fonds.FOND_TRANSACTIONS
WHERE TX_TS > TO_TIMESTAMP(:wm, 'YYYY-MM-DD HH24:MI:SS')
ORDER BY TX_TS
'''
cur.execute(sql, wm=last_wm)
cols = [d[0] for d in cur.description]
rows = cur.fetchall()
if not rows:
    print("No new rows since", last_wm)
    raise SystemExit(0)

import pyarrow as pa, pyarrow.parquet as pq
table = pa.Table.from_pylist([dict(zip(cols, r)) for r in rows])
max_tx_ts = max([r[-1] for r in rows])
ymd = max_tx_ts.date().strftime("%Y/%m/%d")

local_file = "batch.parquet"
pq.write_table(table, local_file)

blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
container = blob_service.get_container_client(AZURE_CONTAINER)

blob_path = f"{PREFIX}/tx_date={ymd}/fond_transactions_{dt.datetime.utcnow().strftime('%Y%m%d%H%M%S')}.parquet"
with open(local_file, "rb") as f:
    container.upload_blob(name=blob_path, data=f)

wm_file.write_text(max_tx_ts.strftime("%Y-%m-%d %H:%M:%S"))
print(f"Uploaded {len(rows)} rows to {blob_path}")
