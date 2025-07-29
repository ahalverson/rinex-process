import pathlib
import pandas as pd
import georinex as gr
from tqdm import tqdm
import warnings
import multiprocessing
import matplotlib.pyplot as plt
import seaborn as sns

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)

# Global parameters list
ALL_PARAMS = [
    'SVclockBias', 'SVclockDrift', 'SVclockDriftRate', 'IODE', 'Crs',
    'Delta_n', 'M0', 'Cuc', 'Eccentricity', 'Cus', 'sqrtA', 'Toe',
    'Cic', 'Omega0', 'Cis', 'Io', 'Crc', 'omega', 'OmegaDot',
    'IDOT', 'TGD', 'SVaccuracy', 'health'
]

def process_rinex_file(file_path: pathlib.Path):
    """Processes a single RINEX file, returning a DataFrame with a 'time' column."""
    try:
        df = gr.load(file_path).to_dataframe(fast=True).reset_index()
        if 'constellation' in df.columns:
            df = df[df['constellation'] == 'G']
        
        required_cols = ['time'] + ALL_PARAMS
        df.dropna(subset=required_cols, inplace=True)

        if not df.empty:
            return ('success', df[required_cols])
        else:
            return ('nodata', str(file_path))
    except Exception as e:
        return ('error', (str(file_path.name), str(e)))

def analyze_volatility(df: pd.DataFrame, start_date: str, end_date: str, period: str, period_name: str):
    """
    Analyzes the volatility (period-over-period changes) of ephemeris parameters.

    Args:
        df: The fully aggregated DataFrame.
        start_date, end_date: The date range for analysis.
        period: The pandas resampling period code ('D', 'W', 'M', 'Y').
        period_name: The human-readable name of the period (e.g., 'Weekly').
    """
    print("\n" + "="*80)
    print(f"--- Analyzing {period_name} Volatility ---")
    
    # Use a copy to avoid modifying the original DataFrame's index
    df_analysis = df.copy()
    df_analysis.set_index('time', inplace=True)
    df_analysis.sort_index(inplace=True)

    # --- 1. Filter by Date Range ---
    start_str = start_date or "start"
    end_str = end_date or "end"
    
    # Use .loc for robust date slicing
    if start_date:
        df_analysis = df_analysis.loc[df_analysis.index >= start_date]
    if end_date:
        df_analysis = df_analysis.loc[df_analysis.index <= end_date]

    if df_analysis.empty:
        print(f"Error: No data found in the specified date range ({start_str} to {end_str}).")
        print("="*80 + "\n")
        return

    # --- 2. Resample and Calculate Period-over-Period Changes ---
    period_means = df_analysis.resample(period).mean()
    period_deltas = period_means.diff().dropna(how='all')

    if period_deltas.empty or len(period_deltas) < 2:
        print(f"Not enough consecutive data for {period_name} analysis in this date range.")
        print("="*80 + "\n")
        return

    # --- 3. Perform Statistical Analysis on the *Changes* ---
    print(f"\n*** Descriptive Statistics of {period_name} *Changes* ({start_str} to {end_str}) ***")
    print(f"This shows the typical size and volatility of {period_name.lower()} adjustments.\n")
    print(period_deltas.describe())

    # --- 4. Visualize the Distribution of Changes ---
    if len(period_deltas) < 10:
        print(f"\nSkipping plot: fewer than 10 data points for {period_name} volatility analysis.")
    else:
        print(f"\nGenerating histogram for {period_name} changes...")
        params_to_plot = ['SVclockDrift', 'Crs', 'SVaccuracy']
        fig, axes = plt.subplots(len(params_to_plot), 1, figsize=(10, 5 * len(params_to_plot)))
        if len(params_to_plot) == 1: axes = [axes]
        
        fig.suptitle(f'Distribution of {period_name} Ephemeris Changes ({start_str} to {end_str})', fontsize=16)

        for i, param in enumerate(params_to_plot):
            if param in period_deltas.columns:
                sns.histplot(period_deltas[param].dropna(), kde=True, ax=axes[i], bins=30)
                axes[i].set_title(f'Distribution of {period_name} Change in {param}')
                axes[i].set_xlabel(f'Change in {param} per {period_name.rstrip("ly")}')
                axes[i].set_ylabel('Frequency')
        
        plt.tight_layout(rect=[0, 0.03, 1, 0.96])
        output_filename = f'{period_name.lower()}_volatility_{start_str}_{end_str}.png'
        plt.savefig(output_filename)
        print(f"Successfully saved change distribution plot to '{output_filename}'")
    
    print("="*80 + "\n")

def get_date_from_user(prompt_text: str) -> str:
    """Gets and validates a date string from the user."""
    while True:
        date_str = input(prompt_text).strip()
        if not date_str: # User pressed Enter for default
            return None
        try:
            # Validate the format
            pd.to_datetime(date_str)
            return date_str
        except ValueError:
            print("Invalid format. Please use YYYY-MM-DD or press Enter to skip.")

def main():
    """Main function to orchestrate the entire process."""
    # !!! IMPORTANT !!!
    YOUR_RINEX_DIRECTORY = '/path/to/cddis_data_archive'
    root_path = pathlib.Path(YOUR_RINEX_DIRECTORY)
    
    if not root_path.is_dir():
        print(f"Error: Directory not found at '{YOUR_RINEX_DIRECTORY}'")
        return

    # --- 1. Data Ingestion (Expensive step, done once) ---
    print("Searching for RINEX navigation files...")
    nav_files = sorted(list(set(
        list(root_path.rglob('*.*n.gz')) + list(root_path.rglob('*.nav.gz')) +
        list(root_path.rglob('*.*n.Z')) + list(root_path.rglob('*.nav.Z')) +
        list(root_path.rglob('*.*n')) + list(root_path.rglob('*.nav'))
    )))
    print(f"Found {len(nav_files)} files.")
    
    if not nav_files: return

    all_data = []
    print(f"\nParsing files in parallel using {multiprocessing.cpu_count()} CPU cores...")
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results_iterator = pool.imap_unordered(process_rinex_file, nav_files)
        for result in tqdm(results_iterator, total=len(nav_files), desc="Processing files"):
            if result[0] == 'success':
                all_data.append(result[1])

    if not all_data
        print("\nError: No valid data could be extracted.")
        return

    print("\nAggregating all data into memory...")
    full_df = pd.concat(all_data, ignore_index=True)
    full_df['time'] = pd.to_datetime(full_df['time']) # Ensure time column is datetime type
    del all_data

    # --- 2. Get User Input for Date Range ---
    print("\n" + "-"*50)
    print("Enter Date Range for Analysis")
    print("Format: YYYY-MM-DD. Press Enter for no limit.")
    start_date = get_date_from_user("Enter start date: ")
    end_date = get_date_from_user("Enter end date: ")
    print("-"*50)

    # --- 3. Loop Through Analysis Periods ---
    periods_to_analyze = {
        'D': 'Daily',
        'W': 'Weekly',
        'M': 'Monthly',
        'Y': 'Yearly'
    }

    for code, name in periods_to_analyze.items():
        analyze_volatility(
            df=full_df, 
            start_date=start_date, 
            end_date=end_date,
            period=code,
            period_name=name
        )
    
    print("Analysis complete.")


if __name__ == '__main__':
    # Set a nice default plotting style
    sns.set_theme(style="whitegrid")
    main()
