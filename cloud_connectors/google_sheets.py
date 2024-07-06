from typing import Union

import pandas as pd
import gspread


def upload_to_google_sheets(input_data: Union[str, pd.DataFrame],
                            sheet_name: str,
                            tab_name: str,
                            start_cell: str):
    """
    Uploads data to a specified Google Sheet.

    :param input_data: str or pd.DataFrame - Path to a CSV/Excel file or a pandas DataFrame.
    :param sheet_name: str - Name of the Google Sheet.
    :param tab_name: str - Name of the tab within the Google Sheet.
    :param start_cell: str - The top-left cell where the data will start.
    """
    # Set up Google Sheets API client
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    client = gspread.service_account(filename='/Users/amihaio/.config/gspread/gg-data-connector-8171c002937a.json')
    sheet = client.open(sheet_name)

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

    # Select the tab
    try:
        worksheet = sheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound as e:
        raise ValueError(
            f"Tab '{tab_name}' not found in Google Sheet '{sheet_name}'."
        ) from e

    # Convert DataFrame to list of lists
    data_list = [data.columns.values.tolist()] + data.values.tolist()

    # Update the worksheet with data
    res = worksheet.update(data_list, start_cell)

    if res['updatedCells'] == len(data_list) * len(data_list[0]):
        print(f"Data uploaded successfully to Google Sheet '{sheet_name}' in tab '{tab_name}'.")
    else:
        print("Failed to upload data to Google Sheet.")


if __name__ == "__main__":
    # Example usage
    df = pd.DataFrame({
        "Name": ["Alice", "Bob", "Charlie"],
        "Age":  [25, 30, 35],
        "City": ["New York", "Los Angeles", "Chicago"]
    })

    upload_to_google_sheets(df, "data-connector-test", "test", "A1")
