from typing import Optional, Union
import os
import logging

import pandas as pd
import gspread
from gspread.utils import ValueInputOption

CREDS_PATH = os.environ.get('credentials_path')

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)


def upload_to_google_sheets(input_data: Union[str, pd.DataFrame],
                            sheet_name: str,
                            tab_name: str,
                            start_cell: str,
                            drop_headers: Optional[bool] = True):
    """
    Uploads data to a specified Google Sheet.

    :param input_data: str or pd.DataFrame - Path to a CSV/Excel file or a pandas DataFrame.
    :param sheet_name: str - Name of the Google Sheet.
    :param tab_name: str - Name of the tab within the Google Sheet.
    :param start_cell: str - The top-left cell where the data will start.
    :param drop_headers: bool - Whether to drop the headers from the DataFrame.
    """
    # Set up Google Sheets API client
    client = gspread.service_account(filename=CREDS_PATH)
    sheet = client.open(sheet_name)
    LOGGER.debug(f"Connected to Google Sheet '{sheet_name}'.")

    # Load data
    if isinstance(input_data, str):
        if input_data.endswith(".csv"):
            data = pd.read_csv(input_data)
        elif input_data.endswith((".xls", ".xlsx")):
            data = pd.read_excel(input_data)
        else:
            raise ValueError("Unsupported file format. Please provide a CSV or Excel file.")
    elif isinstance(input_data, pd.DataFrame):
        data = input_data
    else:
        raise ValueError("Invalid input_data type. Please provide a file path or a pandas DataFrame.")

    data = data.fillna("")

    try:
        worksheet = sheet.worksheet(tab_name)
        LOGGER.debug(f"Connected to tab '{tab_name}'.")
    except gspread.exceptions.WorksheetNotFound as e:
        raise ValueError(
            f"Tab '{tab_name}' not found in Google Sheet '{sheet_name}'."
        ) from e

    # Convert DataFrame to list of lists
    data_list = [data.columns.values.tolist()] + data.values.tolist()
    if drop_headers:
        data_list = data_list[1:]

    # Update the worksheet with data
    res = worksheet.update(data_list, start_cell, value_input_option=ValueInputOption.RAW)
    LOGGER.debug(f"Uploaded data to Google Sheet '{sheet_name}' in tab '{tab_name}'.")

    if res['updatedCells'] == len(data_list) * len(data_list[0]):
        print(f"Data uploaded successfully to Google Sheet '{sheet_name}' in tab '{tab_name}'.")
    else:
        print("Failed to upload data to Google Sheet.")


def read_from_google_sheets(sheet_name: str,
                            tab_name: str,
                            start_cell: Optional[int] = None,
                            num_rows: Optional[int] = None,
                            num_cols: Optional[int] = None):
    """
    Reads data from a specified Google Sheet.

    :param sheet_name: str - Name of the Google Sheet.
    :param tab_name: str - Name of the tab within the Google Sheet.
    :param start_cell: str - The top-left cell where the data starts.
    :param num_rows: int - Number of rows to read.
    :param num_cols: int - Number of columns to read.
    :return: pd.DataFrame - Data read from the Google Sheet.
    """
    # Set up Google Sheets API client
    client = gspread.service_account(filename=CREDS_PATH)
    sheet = client.open(sheet_name)

    # Select the tab
    try:
        worksheet = sheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound as e:
        raise ValueError(
            f"Tab '{tab_name}' not found in Google Sheet '{sheet_name}'."
        ) from e

    # Read data from the worksheet
    data = worksheet.get_all_values()

    # Convert data to a DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])

    if start_cell and num_rows and num_cols:
        df = df.iloc[start_cell:start_cell + num_rows, start_cell:start_cell + num_cols]

    return df


if __name__ == "__main__":
    # Example usage
    df = pd.DataFrame({
        "Name": ["Alice", "Bob", "Charlie"],
        "Age":  [25, 30, 35],
        "City": ["New York", "Los Angeles", "Chicago"]
    })

    upload_to_google_sheets(df, "data-connector-test", "test", "A1")
