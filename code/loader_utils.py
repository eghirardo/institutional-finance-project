import wrds
import pandas as pd
from typing import Union, Literal

def get_taq_data(
    conn: wrds.Connection, 
    ticker: str, 
    date: str, 
    data_type: Literal['trades', 'quotes'],
    time_start: str = '9:30:00', 
    time_end: str = '16:00:00',
    library: str = 'taqmsec',
    verbosity: int = 1
) -> Union[pd.DataFrame, None]:
    """
    Retrieves high-frequency TAQ transaction (trades) or quote data for a 
    specific ticker and date.

    Args:
        conn: The active WRDS connection object.
        ticker: The stock ticker symbol (e.g., 'AAPL').
        date: The date in 'YYYY-MM-DD' format (e.g., '2023-09-01').
        data_type: Must be 'trades' for transactions or 'quotes' for quotes.
        time_start: Start time for the data window (e.g., '9:30:00').
        time_end: End time for the data window (e.g., '16:00:00').
        library: The TAQ library to query ('taqmsec' recommended for millisecond data).

    Returns:
        A pandas DataFrame with the requested data, or None if the query fails.
    """
    
    if data_type not in ['trades', 'quotes']:
        if verbosity > 0:
            print("Error: data_type must be 'trades' or 'quotes'.")
        return None

    # 1. Format the date for the table name (YYYYMMDD) and SQL filter (YYYY-MM-DD)
    try:
        date_sql = pd.to_datetime(date).strftime('%Y-%m-%d') 
        table_date = pd.to_datetime(date).strftime('%Y%m%d')
    except ValueError:
        if verbosity > 0:
            print(f"Error: Invalid date format for {date}. Please use 'YYYY-MM-DD'.")
        return None
    
    # 2. Define table name prefix and columns based on data_type
    if data_type == 'trades':
        # CTM tables for Trades
        table_prefix = 'ctm'
        select_cols = """
            price,         -- Trade price
            size,          -- Trade size
            tr_scond,      -- Trade sale condition
            tr_corr        -- Trade correction indicator
        """
        trade_filters = "AND tr_corr = '00' AND price > 0 AND size > 0"
    
    elif data_type == 'quotes':
        # CQM tables for Quotes
        table_prefix = 'cqm'
        select_cols = """
            bid,           -- Best Bid Price
            bidsiz,        -- Best Bid Size
            ask,           -- Best Ask Price
            asksiz,        -- Best Ask Size
            qu_cond       -- Quote Condition
        """
        trade_filters = "" # Quotes typically don't need the same price/size filters
        
    # Construct the full table name
    table_name = f'{table_prefix}_{table_date}'
    
    # 3. Define the SQL Query
    sql_query = f"""
    SELECT 
        DATE_TRUNC('second', date) + time_m AS datetime, -- Combine for a single timestamp
        ex,              -- Exchange code
        sym_root,        -- Ticker symbol
        {select_cols}
    FROM 
        {library}.{table_name}
    WHERE 
        sym_root = '{ticker}'
        AND date = '{date_sql}'
        AND time_m >= '{time_start}'::time 
        AND time_m <= '{time_end}'::time 
        {trade_filters} 
    """
    
    if verbosity > 1:
        print(f"Executing query for {ticker} {data_type} on {date}...")
    
    # 4. Execute the query using raw_sql()
    try:
        data = conn.raw_sql(sql_query, date_cols=['datetime'])
        # Set the correct index
        if not data.empty:
            data = data.set_index('datetime')
        if verbosity > 1:
            print(f"Successfully retrieved {len(data)} {data_type}.")
        return data
    except Exception as e:
        if verbosity > 0:
            print(f"Error retrieving {data_type} data for {ticker} on {date}: {e}")
            print("This often means the TAQ table for that date doesn't exist, or you don't have access.")
        return None

def get_taq_data_range(
    conn: wrds.Connection,
    ticker: str,
    start_date: str,
    end_date: str,
    data_type: Literal['trades', 'quotes'],
    time_start: str = '9:30:00',
    time_end: str = '16:00:00',
    library: str = 'taqmsec',
    verbosity: int = 1
) -> Union[pd.DataFrame, None]:
    """
    Retrieves and merges TAQ data for a ticker over a date range.
    Handles missing days gracefully (skips days with no data).

    Args:
        conn: The active WRDS connection object.
        ticker: The stock ticker symbol (e.g., 'AAPL').
        start_date: Start date in 'YYYY-MM-DD' format.
        end_date: End date in 'YYYY-MM-DD' format.
        data_type: 'trades' or 'quotes'.
        time_start: Start time for the data window.
        time_end: End time for the data window.
        library: TAQ library to query.

    Returns:
        Merged pandas DataFrame with all available data in the interval, or None if no data found.
    """
    try:
        date_range = pd.date_range(start=start_date, end=end_date)
    except Exception as e:
        if verbosity > 0:
            print(f"Error creating date range: {e}")
        return None

    dfs = []
    for date in date_range:
        date_str = date.strftime('%Y-%m-%d')
        df = get_taq_data(
            conn=conn,
            ticker=ticker,
            date=date_str,
            data_type=data_type,
            time_start=time_start,
            time_end=time_end,
            library=library,
            verbosity=verbosity
        )
        if df is not None and not df.empty:
            dfs.append(df)
        else:
            if verbosity > 1:
                print(f"No data for {ticker} on {date_str} (skipping)")

    if dfs:
        merged = pd.concat(dfs)
        if verbosity > 0:
            print(f"Merged {len(dfs)} days of {data_type} data for {ticker}.")
        return merged
    else:
        if verbosity > 0:
            print(f"No data found for {ticker} in range {start_date} to {end_date}.")
        return None

