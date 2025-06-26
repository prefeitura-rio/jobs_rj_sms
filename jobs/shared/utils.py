import pandas as pd
import os
import uuid
import shutil
import glob
import basedosdados as bd
from typing import Optional, Literal


def log(message: str, level: str = "info"):
    print(f"{level}: {message}")

def foo():
    log("foo")


def safe_export_df_to_parquet(df: pd.DataFrame, output_path: str) -> str:
    """
    Safely exports a DataFrame to a Parquet file.

    Args:
        df (pd.DataFrame): The DataFrame to export.
        output_path (str): The path to the output Parquet file.

    Returns:
        str: The path to the output Parquet file.
    """
    df.to_csv(output_path.replace("parquet", "csv"), index=False)

    dataframe = pd.read_csv(
        output_path.replace("parquet", "csv"),
        sep=",",
        dtype=str,
        keep_default_na=False,
        encoding="utf-8",
    )
    dataframe.to_parquet(output_path, index=False)

    # Delete the csv file
    os.remove(output_path.replace("parquet", "csv"))
    return output_path

def create_date_partitions(
    dataframe,
    partition_column,
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder="./data/",
):

    dataframe[partition_column] = pd.to_datetime(dataframe[partition_column])
    dataframe["data_particao"] = dataframe[partition_column].dt.strftime("%Y-%m-%d")

    dates = dataframe["data_particao"].unique()
    dataframes = [
        (
            date,
            dataframe[dataframe["data_particao"] == date].drop(columns=["data_particao"]),
        )  # noqa
        for date in dates
    ]

    for _date, dataframe in dataframes:
        partition_folder = os.path.join(
            root_folder, f"ano_particao={_date[:4]}/mes_particao={_date[5:7]}/data_particao={_date}"
        )
        os.makedirs(partition_folder, exist_ok=True)

        file_folder = os.path.join(partition_folder, f"{uuid.uuid4()}.{file_format}")

        if file_format == "csv":
            dataframe.to_csv(file_folder, index=False)
        elif file_format == "parquet":
            safe_export_df_to_parquet(df=dataframe, output_path=file_folder)

    return root_folder

def upload_to_datalake(
    input_path: str,
    dataset_id: str,
    table_id: str,
    dump_mode: str = "append",
    source_format: str = "csv",
    csv_delimiter: str = ";",
    if_exists: str = "replace",
    if_storage_data_exists: str = "replace",
    biglake_table: bool = True,
    dataset_is_public: bool = False,
    exception_on_missing_input_file: bool = False,
):
    """
    Uploads data to a Google Cloud Storage bucket and creates or appends to a BigQuery table.

    Args:
        input_path (str): The path to the input data file. It can be a folder or a file.
        dataset_id (str): The ID of the BigQuery dataset.
        table_id (str): The ID of the BigQuery table.
        dump_mode (str, optional): The dump mode for the table. Defaults to "append". Accepted values are "append" and "overwrite".
        source_format (str, optional): The format of the input data. Defaults to "csv". Accepted values are "csv" and "parquet".
        csv_delimiter (str, optional): The delimiter used in the CSV file. Defaults to ";".
        if_exists (str, optional): The behavior if the table already exists. Defaults to "replace".
        if_storage_data_exists (str, optional): The behavior if the storage data already exists. Defaults to "replace".
        biglake_table (bool, optional): Whether the table is a BigLake table. Defaults to True.
        dataset_is_public (bool, optional): Whether the dataset is public. Defaults to False.

    Raises:
        RuntimeError: If an error occurs during the upload process.

    Returns:
        None
    """

    if input_path == "":
        log("Received input_path=''. No data to upload", level="warning")
        if exception_on_missing_input_file:
            raise FileNotFoundError(f"No files found in {input_path}")
        return

    # If Input path is a folder
    if os.path.isdir(input_path):
        log(f"Input path is a folder: {input_path}")

        reference_path = os.path.join(input_path, f"**/*.{source_format}")
        log(f"Reference Path: {reference_path}")

        if len(glob.glob(reference_path, recursive=True)) == 0:
            log(f"No files found in {input_path}", level="warning")
            if exception_on_missing_input_file:
                raise FileNotFoundError(f"No files found in {input_path}")
            return

    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    table_staging = f"{tb.table_full_name['staging']}"
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
    storage_path_link = (
        f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
        f"/staging/{dataset_id}/{table_id}"
    )
    log(f"Uploading file {input_path} to {storage_path} with {source_format} format")

    try:
        table_exists = tb.table_exists(mode="staging")

        if not table_exists:
            log(f"CREATING TABLE: {dataset_id}.{table_id}")
            tb.create(
                path=input_path,
                source_format=source_format,
                csv_delimiter=csv_delimiter,
                if_storage_data_exists=if_storage_data_exists,
                biglake_table=biglake_table,
                dataset_is_public=dataset_is_public,
            )
        else:
            if dump_mode == "append":
                log(f"TABLE ALREADY EXISTS APPENDING DATA TO STORAGE: {dataset_id}.{table_id}")

                tb.append(filepath=input_path, if_exists=if_exists)
            elif dump_mode == "overwrite":
                log(
                    "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                    f"{storage_path}\n"
                    f"{storage_path_link}"
                )  # pylint: disable=C0301
                st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
                log(
                    "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                    f"{storage_path}\n"
                    f"{storage_path_link}"
                )  # pylint: disable=C0301
                tb.delete(mode="all")
                log(
                    "MODE OVERWRITE: Sucessfully DELETED TABLE:\n" f"{table_staging}\n"
                )  # pylint: disable=C0301

                tb.create(
                    path=input_path,
                    source_format=source_format,
                    csv_delimiter=csv_delimiter,
                    if_storage_data_exists=if_storage_data_exists,
                    biglake_table=biglake_table,
                    dataset_is_public=dataset_is_public,
                )
        log("Data uploaded to BigQuery")

    except Exception as e:  # pylint: disable=W0703
        log(f"An error occurred: {e}", level="error")
        raise RuntimeError() from e

def upload_df_to_datalake(
    df: pd.DataFrame,
    dataset_id: str,
    table_id: str,
    dump_mode: str = "append",
    source_format: str = "csv",
    csv_delimiter: str = ";",
    partition_column: Optional[str] = None,
    if_exists: str = "replace",
    if_storage_data_exists: str = "replace",
    biglake_table: bool = True,
    dataset_is_public: bool = False,
):
    root_folder = f"./data/{uuid.uuid4()}"
    os.makedirs(root_folder, exist_ok=True)
    log(f"Using as root folder: {root_folder}")

    # All columns as strings
    df = df.astype(str)
    log("Converted all columns to strings")

    if partition_column:
        log(f"Creating date partitions for a {df.shape[0]} rows dataframe")
        partition_folder = create_date_partitions(
            dataframe=df,
            partition_column=partition_column,
            file_format=source_format,
            root_folder=root_folder,
        )
    else:
        log(f"Creating a single partition for a {df.shape[0]} rows dataframe")
        file_path = os.path.join(root_folder, f"{uuid.uuid4()}.{source_format}")
        if source_format == "csv":
            df.to_csv(file_path, index=False)
        elif source_format == "parquet":
            safe_export_df_to_parquet(df=df, output_path=file_path)
        partition_folder = root_folder

    log(f"Uploading data to partition folder: {partition_folder}")
    upload_to_datalake(
        input_path=partition_folder,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        source_format=source_format,
        csv_delimiter=csv_delimiter,
        if_exists=if_exists,
        if_storage_data_exists=if_storage_data_exists,
        biglake_table=biglake_table,
        dataset_is_public=dataset_is_public,
        exception_on_missing_input_file=True,
    )

    # Delete files from partition folder
    log(f"Deleting partition folder: {root_folder}")
    shutil.rmtree(root_folder)
    return