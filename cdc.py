import pandas as pd
import apache_beam as beam
from google.cloud import bigquery
from sqlalchemy import create_engine, text
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--dataset", required=True)
        parser.add_argument("--table", required=True)

def fetch_latest_after_per_idproduk():
    engine = create_engine("mssql+pymssql://username:password@your_ipaddress:port/yourdb")

    with engine.begin() as conn:
        conn.execute(text("EXEC dbo.usp_capture_produk_cdc"))

    df = pd.read_sql("SELECT * FROM dbo.staging_produk_cdc", engine)
    after_df = df[df["operasi"] == "UPDATE - AFTER"]
    latest_df = after_df.sort_values("tanggalupdate").drop_duplicates(subset=["idproduk"], keep="last")
    latest_df["tanggalupdate"] = latest_df["tanggalupdate"].astype(str)
    latest_df = latest_df[["idproduk", "namaproduk", "hargaproduk", "tanggalupdate"]]

    return latest_df.to_dict(orient="records")


class DeleteFromBigQueryFn(beam.DoFn):
    def __init__(self, project, dataset, table):
        self.project = project
        self.dataset = dataset
        self.table = table

    def setup(self):
        # IMPORT DI DALAM setup(), agar dikenali di konteks worker Dataflow
        from google.cloud import bigquery
        self.bigquery = bigquery  # simpan modul sebagai instance var
        self.client = bigquery.Client(project=self.project)

    def process(self, element):
        table_id = f"{self.project}.{self.dataset}.{self.table}"
        row_id = element["idproduk"]
        query = f"DELETE FROM `{table_id}` WHERE idproduk = @id"
        job_config = self.bigquery.QueryJobConfig(
            query_parameters=[
                self.bigquery.ScalarQueryParameter("id", "INT64", row_id)
            ]
        )
        self.client.query(query, job_config=job_config).result()
        yield element


def run():
    options = CustomOptions()
    gcp_opts = options.view_as(GoogleCloudOptions)

    project = gcp_opts.project
    dataset = options.dataset
    table = options.table
    table_full_id = f"{project}:{dataset}.{table}"

    latest_data = fetch_latest_after_per_idproduk()

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Create elements" >> beam.Create(latest_data)
            | "Delete old rows" >> beam.ParDo(DeleteFromBigQueryFn(project, dataset, table))
            | "Insert latest rows" >> WriteToBigQuery(
                table=table_full_id,
                schema="idproduk:INTEGER, namaproduk:STRING, hargaproduk:FLOAT, tanggalupdate:DATETIME",
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    run()
