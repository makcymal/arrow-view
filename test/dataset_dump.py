import pandas as pd
import pyarrow as pa

df = pd.read_csv("dataset.csv")
table = pa.Table.from_pandas(df)
with pa.ipc.new_file("dataset.arrow", table.schema) as writer:
  writer.write(table)
