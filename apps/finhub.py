from typing import Optional

import pandas as pd

from cloud_connectors.google_sheets import read_from_google_sheets, upload_to_google_sheets


def days_in_month(year, month):
    # Check for February and leap year
    if month == 2:
        if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
            return 29
        else:
            return 28
    # April, June, September, and November have 30 days
    elif month in [4, 6, 9, 11]:
        return 30
    # All other months have 31 days
    else:
        return 31


class TransTableCols:
    DATE = 'Date'
    PAYEE = 'Payee'
    AMOUNT = 'Amount'
    CATEGORY = 'Category'
    ACCOUNT = 'Account'
    ID = 'id'


def read_transactions_table(start_date: Optional[str] = None, end_date: Optional[str] = None):
    data = read_from_google_sheets('FinHub', 'Transactions')
    if start_date:
        data = data[data[TransTableCols.DATE] >= pd.to_datetime(start_date)]
    if end_date:
        data = data[data[TransTableCols.DATE] <= pd.to_datetime(end_date)]
    return data


def upload_to_transactions_table(data: pd.DataFrame, mm_yyyy: str):
    days_in_given_month = days_in_month(int(mm_yyyy.split('_')[0]), int(mm_yyyy.split('_')[1]))
    transactions = read_transactions_table(start_date=f'{mm_yyyy}-01', end_date=f'{mm_yyyy}-{days_in_given_month}')
    transactions = transactions.append(data)
    transactions = transactions.drop_duplicates(subset=[TransTableCols.ID])
    upload_to_google_sheets(transactions, 'FinHub', 'Transactions')
