from typing import Optional
import logging

import pandas as pd

from cloud_connectors.connectors.google_sheets import read_from_google_sheets, upload_to_google_sheets

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

LOGGER = logging.getLogger(__name__)


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
    TRANSACTION_DATE = 'transaction date'
    BILLING_DATE = 'billing date'
    ORIGNAL_CURRENCY = 'original currency'
    EXCHANGE_RATE = 'exchange rate'
    PAYEE = 'payee'
    AMOUNT = 'amount'
    MEMO = 'memo'
    CATEGORY = 'category'
    ACCOUNT = 'account'
    ID = 'id'


SHEETNAME = 'FinHub'
TABNAME = 'TransactionsDB'


def _apply_transactions_table_dtypes(data: pd.DataFrame):
    data[TransTableCols.TRANSACTION_DATE] = pd.to_datetime(data[TransTableCols.TRANSACTION_DATE])
    data[TransTableCols.BILLING_DATE] = pd.to_datetime(data[TransTableCols.BILLING_DATE])
    data[TransTableCols.AMOUNT] = pd.to_numeric(data[TransTableCols.AMOUNT])
    data[TransTableCols.ID] = data[TransTableCols.ID].astype(str)
    data[TransTableCols.PAYEE] = data[TransTableCols.PAYEE].astype(str)
    return data


def read_transactions_table(start_date: Optional[str] = None, end_date: Optional[str] = None):
    data = read_from_google_sheets(SHEETNAME, TABNAME)
    data[TransTableCols.TRANSACTION_DATE] = pd.to_datetime(data[TransTableCols.TRANSACTION_DATE])
    if start_date:
        data = data[data[TransTableCols.TRANSACTION_DATE] >= pd.to_datetime(start_date)]
    if end_date:
        data = data[data[TransTableCols.TRANSACTION_DATE] <= pd.to_datetime(end_date)]
    return data


def upload_to_transactions_table(new_trans: pd.DataFrame, year_month: str):
    """
    Upload transactions to transaction table for a given month
    :param new_trans:
    :param year_month: format: 'YYYY-MM'
    :return:
    """
    days_in_given_month = days_in_month(int(year_month.split('-')[0]),
                                        int(year_month.split('-')[1]))
    all_transactions = read_transactions_table()
    LOGGER.debug(f"Read {len(all_transactions)} transactions from transactions table.")
    cond1 = all_transactions[TransTableCols.TRANSACTION_DATE] >= pd.to_datetime(f'{year_month}-01')
    cond2 = all_transactions[TransTableCols.TRANSACTION_DATE] <= pd.to_datetime(f'{year_month}-{days_in_given_month}')
    curr_month_trans = all_transactions[cond1 & cond2]
    curr_month_trans = pd.concat([curr_month_trans, new_trans], ignore_index=True)
    LOGGER.debug(f'Length of current month transactions before dedup: {len(curr_month_trans)}')
    curr_month_trans = curr_month_trans.drop_duplicates(subset=[TransTableCols.ID], keep=False)
    LOGGER.debug(f'Length of current month transactions after dedup: {len(curr_month_trans)}')
    curr_month_trans[TransTableCols.TRANSACTION_DATE] = curr_month_trans[TransTableCols.TRANSACTION_DATE].astype(str)
    curr_month_trans[TransTableCols.BILLING_DATE] = curr_month_trans[TransTableCols.BILLING_DATE].astype(str)

    if not curr_month_trans.empty:
        LOGGER.debug(f"Uploading {len(curr_month_trans)} transactions to transactions table.")
        upload_to_google_sheets(curr_month_trans, SHEETNAME, TABNAME, f"A{len(all_transactions) + 2}")


if __name__ == '__main__':
    # SHEETNAME = 'data-connector-test'
    test_data = {
        "Date":               ["27/6/24", "26/6/24", "25/6/24", "24/6/24", "23/6/24"],
        "Payee":              ["Supermarket", "Gym Membership", "Electric Company", "Internet Provider", "Restaurant"],
        "Category":           ["Food: Supermarket", "Health: Gym", "Bills: Electricity", "Bills: Internet",
                               "Food: Dining Out"],
        "Amount":             ["150.00", "200.00", "350.00", "100.00", "250.00"],
        "Original Amount":    ["150.00", "200.00", "350.00", "100.00", "250.00"],
        "Memo":               ["", "", "", "", ""],
        "id":                 ["45439_Supermarket_150", "45438_Gym Membership_200", "45437_Electric Company_350",
                               "45436_Internet Provider_100", "45435_Restaurant_250"],
        "Duplicate":          ["", "", "", "", ""],
        "Account":            ["Amihai CC", "Amihai CC", "Amihai CC", "Amihai CC", "Amihai CC"],
        "Duplicates":         ["", "", "", "", ""],
        "Helper For Savings": ["-150", "-200", "-350", "-100", "-250"]
    }

    df = pd.DataFrame(test_data)
    upload_to_transactions_table(df, '2024-06')
