#!/usr/bin/env python3
"""
Parse grep output into CSV and load as pandas DataFrame.
"""

import sys
import re
import csv
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from typing import Tuple, List
from typing import List, Tuple, Optional


def parse_grep_output(grep_text: str, delimiter: str = ":", max_fields: Optional[int] = None) -> Tuple[List[str], List[dict]]:
    """
    Parse grep output into structured data.
    
    Args:
        grep_text: Raw grep output text
        delimiter: Field delimiter (default ':' for standard grep format)
        max_fields: Maximum fields to split (None = all)
    
    Returns:
        Tuple of (column_names, list of row dictionaries)
    """
    lines = grep_text.strip().split('\n')
    
    if not lines:
        return [], []
    
    # Parse first line to determine column names
    first_line_parts = lines[0].split(delimiter, maxsplit=max_fields if max_fields else -1)
    
    # Common grep output columns: filename:line_number:content
    # Adjust based on grep options used
    if len(first_line_parts) >= 3:
        columns = ['filename', 'line_number', 'content']
    elif len(first_line_parts) == 2:
        columns = ['filename', 'content']
    else:
        columns = ['match']
    
    # Add extra columns if more fields exist
    while len(columns) < len(first_line_parts):
        columns.append(f'field_{len(columns)}')
    
    rows = []
    for line in lines:
        if not line.strip():
            continue
        
        parts = line.split(delimiter, maxsplit=max_fields if max_fields else -1)
        
        # Pad with empty strings if fewer fields than columns
        while len(parts) < len(columns):
            parts.append('')
        
        row = {col: part for col, part in zip(columns, parts[:len(columns)])}
        rows.append(row)
    
    return columns, rows


def parse_order_update_log(log_text: str) -> Tuple[List[str], List[dict]]:
    """
    Parse order update log format:
    [OrdUpdate:SYMBOL TIMESTAMP] OrderID,ExchOrderID,Time,Price,Qty,Side,Status,Field1,Field2,Field3
    
    Args:
        log_text: Raw log text
    
    Returns:
        Tuple of (column_names, list of row dictionaries)
    """
    import re
    
    lines = log_text.strip().split('\n')
    
    if not lines:
        return [], []
    
    columns = ['symbol', 'log_timestamp', 'order_id', 'exchange_order_id', 'time', 'price', 
               'quantity', 'side', 'status', 'field1', 'field2', 'field3']
    
    rows = []
    pattern = r'\[OrdUpdate:(\S+)\s+([\d:.]+)\]\s+(.+)'
    
    for line in lines:
        if not line.strip():
            continue
        
        match = re.match(pattern, line)
        if not match:
            continue
        
        symbol = match.group(1)
        log_timestamp = match.group(2)
        data_str = match.group(3)
        
        # Parse CSV data
        data_parts = data_str.split(',')
        
        # Ensure we have all fields
        while len(data_parts) < len(columns) - 2:
            data_parts.append('')
        
        row = {
            'symbol': symbol,
            'log_timestamp': log_timestamp,
            'order_id': data_parts[0] if len(data_parts) > 0 else '',
            'exchange_order_id': data_parts[1] if len(data_parts) > 1 else '',
            'time': data_parts[2] if len(data_parts) > 2 else '',
            'price': data_parts[3] if len(data_parts) > 3 else '',
            'quantity': data_parts[4] if len(data_parts) > 4 else '',
            'side': data_parts[5] if len(data_parts) > 5 else '',
            'status': data_parts[6] if len(data_parts) > 6 else '',
            'field1': data_parts[7] if len(data_parts) > 7 else '',
            'field2': data_parts[8] if len(data_parts) > 8 else '',
            'field3': data_parts[9] if len(data_parts) > 9 else '',
        }
        rows.append(row)
    
    return columns, rows


def log_to_csv(log_text: str, output_file: str, log_format: str = "auto", **kwargs) -> pd.DataFrame:
    """
    Convert structured log output to CSV file and return as pandas DataFrame.
    
    Args:
        log_text: Raw log text
        output_file: Path to output CSV file
        log_format: Log format type ("ordupdate", "grep", or "auto")
        **kwargs: Additional arguments passed to pandas.to_csv()
    
    Returns:
        pandas DataFrame containing the parsed data
    """
    if log_format == "auto":
        if "[OrdUpdate:" in log_text:
            log_format = "ordupdate"
        else:
            log_format = "grep"
    
    if log_format == "ordupdate":
        columns, rows = parse_order_update_log(log_text)
    else:
        columns, rows = parse_grep_output(log_text)
    
    if not rows:
        print("No data to parse from log output")
        return pd.DataFrame()
    
    # Create DataFrame
    df = pd.DataFrame(rows)
    
    # Convert numeric columns
    numeric_cols = ['price', 'quantity', 'exchange_order_id']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='ignore')
    
    # Write to CSV
    csv_kwargs = {'index': False, 'quoting': csv.QUOTE_ALL}
    csv_kwargs.update(kwargs)
    df.to_csv(output_file, **csv_kwargs)
    
    print(f"✓ Saved {len(rows)} rows to {output_file}")
    return df



def grep_from_command(command: str, output_file: str, delimiter: str = ":") -> pd.DataFrame:
    """
    Execute grep command and convert output to CSV.
    
    Args:
        command: Full grep command to execute
        output_file: Path to output CSV file
        delimiter: Field delimiter
    
    Returns:
        pandas DataFrame containing the parsed data
    """
    import subprocess
    
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    if result.returncode not in [0, 1]:  # grep returns 1 if no matches found
        print(f"Error executing command: {result.stderr}")
        return pd.DataFrame()
    
    return grep_to_csv(result.stdout, output_file, delimiter)


def plot_price_vs_time(df: pd.DataFrame, output_file: str = "price_vs_time.png", 
                       symbol_col: str = "symbol", price_col: str = "price", 
                       time_col: str = "time") -> None:
    """
    Plot price versus timestamp from order update DataFrame.
    
    Args:
        df: DataFrame containing order data
        output_file: Path to save the plot image
        symbol_col: Column name containing symbol
        price_col: Column name containing price
        time_col: Column name containing timestamp
    """
    if df.empty or price_col not in df.columns or time_col not in df.columns:
        print("Error: DataFrame is empty or missing required columns")
        return
    
    # Create figure
    fig, ax = plt.subplots(figsize=(14, 6))
    
    # Convert price and time to numeric types
    df_plot = df.copy()
    df_plot[price_col] = pd.to_numeric(df_plot[price_col], errors='coerce')

    # Only plot rows whose status contains 'FILL' (e.g., FILLED, PARTIALLY_FILLED)
    if 'status' in df_plot.columns:
        df_plot = df_plot[df_plot['status'].astype(str).str.contains('FILL', case=False, na=False)]
        if df_plot.empty:
            print("No rows with status containing 'FILL' to plot")
            return
    
    # Parse time column - handle HH:MM:SS.mmm format
    try:
        df_plot['time_parsed'] = pd.to_datetime(df_plot[time_col], format='%H:%M:%S.%f')
    except:
        try:
            df_plot['time_parsed'] = pd.to_datetime(df_plot[time_col], format='%H:%M:%S')
        except:
            print("Error: Could not parse time column")
            return
    
    # Group by symbol if available
    if symbol_col in df_plot.columns:
        symbols = df_plot[symbol_col].unique()
        colors = plt.cm.tab10(range(len(symbols)))
        
        for idx, symbol in enumerate(symbols):
            symbol_data = df_plot[df_plot[symbol_col] == symbol].sort_values('time_parsed')
            ax.plot(symbol_data['time_parsed'], symbol_data[price_col], 
                   marker='o', label=symbol, color=colors[idx], linewidth=2, markersize=5)
    else:
        df_plot_sorted = df_plot.sort_values('time_parsed')
        ax.plot(df_plot_sorted['time_parsed'], df_plot_sorted[price_col], 
               marker='o', label='Price', linewidth=2, markersize=5)
    
    # Format plot
    ax.set_xlabel('Time', fontsize=12, fontweight='bold')
    ax.set_ylabel('Price (USD)', fontsize=12, fontweight='bold')
    ax.set_title('Price vs Timestamp', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='best', fontsize=10)
    
    # Format x-axis with time
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate(rotation=45, ha='right')
    
    # Tight layout
    plt.tight_layout()
    
    # Save and display
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"✓ Plot saved to {output_file}")
    plt.show()



if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python grep_to_pandas.py <input_file> <output_csv> [--format {auto|grep|ordupdate}] [--plot]")
        print("\nFormats:")
        print("  auto       - Detect format automatically (default)")
        print("  grep       - Standard grep output: filename:line:content")
        print("  ordupdate  - Order update logs: [OrdUpdate:SYMBOL TIME] fields...")
        print("\nOptions:")
        print("  --plot     - Generate price vs timestamp plot (for ordupdate format)")
        print("\nExamples:")
        print("  python grep_to_pandas.py orders.log orders.csv --format ordupdate --plot")
        print("  python grep_to_pandas.py grep_results.txt results.csv --format grep")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_csv = sys.argv[2]
    
    # Parse additional arguments
    log_format = "auto"
    delimiter = ":"
    plot_enabled = False
    
    i = 3
    while i < len(sys.argv):
        if sys.argv[i] == "--format" and i + 1 < len(sys.argv):
            log_format = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == "--delimiter" and i + 1 < len(sys.argv):
            delimiter = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == "--plot":
            plot_enabled = True
            i += 1
        else:
            i += 1
    
    # Read input file
    try:
        with open(input_file, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found")
        sys.exit(1)
    
    # Parse and save
    df = log_to_csv(content, output_csv, log_format=log_format)
    print(f"\nDataFrame shape: {df.shape}")
    print("\nFirst 5 rows:")
    print(df.head())
    
    # Plot if requested
    if plot_enabled:
        plot_file = output_csv.replace('.csv', '.png')
        plot_price_vs_time(df, output_file=plot_file)
