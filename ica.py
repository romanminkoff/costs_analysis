import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

mon_fmt = '%Y-%m'

def _ica_file(fname):
    return pd.read_xml(fname, xpath='.//transactions')

def all_receipts(butik_kvitto_files: list) -> pd.DataFrame:
    """Works with 'Butik kvitto.xml' files"""
    df = pd.DataFrame()
    for f in butik_kvitto_files:
        df = pd.concat([_ica_file(f), df], axis=0)
    rm_col = ['marketingName', 'vatAmount', 'paymentType']
    df = df.drop(columns=rm_col)
    df.transactionTimestamp = pd.to_datetime(df.transactionTimestamp)
    df = df.sort_values(by=['transactionTimestamp'])
    return df.reset_index(drop=True)

def all_items(butik_kvittorader_files: list) -> pd.DataFrame:
    """Works with 'Butik kvittorader.xml' files"""
    df = pd.DataFrame()
    for f in butik_kvittorader_files:
        df = pd.concat([_ica_file(f), df], axis=0)
    return df

def plot_aggr_monthly_totals(df, time_col='transactionTimestamp',
        val_col='transactionValue'):
    df[[time_col, val_col]].groupby(
        pd.Grouper(key=time_col, freq='1M')).sum()[val_col].plot()

def plot_df_rows(df, title, n_plots=5):
    fig, axs = plt.subplots(n_plots, sharex=True, figsize=(15,2*n_plots))
    fig.suptitle(title)
    plt.xticks(rotation=45, ha="right")

    for i in range(n_plots):
        axs[i].plot(df.iloc[i])
        title=df.iloc[i].name
        axs[i].set_title(title)

def _unique_months(df, fmt=mon_fmt):
    return sorted(set(df.transactionTimestamp.dt.strftime(fmt).values))

def _unique_items(df):
    x = set(df.itemDesc.values)
    if None in x:
        x.remove(None)
    return sorted(x)

def analytics_tables(df_receipts, df_items):
    columns_months = _unique_months(df_receipts)
    idx_items = _unique_items(df_items)
    df_mon_prices = pd.DataFrame(0, columns=columns_months, index=idx_items)
    df_mon_quantity = pd.DataFrame(0, columns=columns_months, index=idx_items)
    df_mon_costs = pd.DataFrame(0, columns=columns_months, index=idx_items)
    return df_mon_prices, df_mon_quantity, df_mon_costs

def set_mon_price(df, mon, item, price) -> None:
    df.at[item, mon] = price

def add_mon_quantity(df, mon, item, quantity) -> None:
    df.at[item, mon] += quantity

def add_mon_cost(df, mon, item, cost) -> None:
    df.at[item, mon] += cost

def sort_by_row_elements_count(df):
    df = df.replace(0.0, np.nan)
    df['count'] = df.count(axis='columns')
    df = df.sort_values(by=['count'], ascending=False)
    df = df.drop(columns=['count'])
    return df.replace(np.nan, 0.0)

def _is_valid_item(desc, quantity):
    return desc and quantity

def _receipt_items(df_items, transactionId):
    return df_items[df_items.transactionId == transactionId]

def analytics(df_receipts, df_items):
    """Sorted tables for prices, quantities and costs."""
    df_prices, df_quantity, df_costs = analytics_tables(df_receipts, df_items)

    for r in df_receipts.itertuples():
        mon = r.transactionTimestamp.strftime(mon_fmt)
        df_receipt_items = _receipt_items(df_items, r.transactionId)
        for it in df_receipt_items.itertuples():
            if _is_valid_item(it.itemDesc, it.quantity):
                price = it.price / it.quantity
                cost = it.price
                set_mon_price(df_prices, mon, it.itemDesc, price)
                add_mon_quantity(df_quantity, mon, it.itemDesc, it.quantity)
                add_mon_cost(df_costs, mon, it.itemDesc, cost)
    return (sort_by_row_elements_count(df_prices),
            sort_by_row_elements_count(df_quantity),
            sort_by_row_elements_count(df_costs))

def items_totals(df_costs, df_quantity):
    """Aggregate costs, item counts and averages"""
    df_sammary = pd.DataFrame(index=df_costs.index)
    df_sammary['sum'] = df_costs.sum(axis=1)
    df_sammary = df_sammary.sort_values(by=['sum'], ascending=False)
    df_sammary['count'] = df_quantity.sum(axis=1)
    df_sammary['avg price'] = df_sammary['sum'] / df_sammary['count']
    n_months = len(df_costs.columns)
    df_sammary['avg per mon'] = df_sammary['sum'] / n_months
    return df_sammary