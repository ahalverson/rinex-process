import pathlib
import pandas as pd
import georinex as gr
from tqdm import tqdm
import warnings
import multiprocessing
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import xarray as xr  # Import xarray

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)

# --- GLOBAL CONFIGURATION ---
CACHE_FILENAME = "gps_ephemeris_cache.nc"
ALL_PARAMS = [
    'SVclockBias', 'SVclockDrift', 'SVclockDriftRate', 'IODE', 'Crs',
    'Delta_n', 'M0', 'Cuc', 'Eccentricity', 'Cus', 'sqrtA', 'Toe',
    'Cic', 'Omega0', 'Cis', 'Io', 'Crc', 'omega', 'OmegaDot',
    'IDOT', 'TGD', 'SVaccuracy', 'health'
]

# --- UNCHANGED WORKER AND ANALYSIS FUNCTIONS ---

def process_rinex_file(file_path: pathlib.Path):
    """Processes a single RINEX file, returning a DataFrame with a 'time' column."""
    try:
        df = gr.load(file_path).to_dataframe(fast=True).reset_index()
        if 'constellation' in df.columns:
            df = df[df['constellation'] == 'G']
        required_cols = ['time'] + ALL_PARAMS
        df.dropna(subset=required_cols, inplace=True)
        return ('success', df[required_cols]) if not df.empty else ('nodata', None)
    except Exception as e:
        return ('error', (str(file_path.name), str(e)))

def analyze_volatility(df: pd.DataFrame, start_date: str, end_date: str, period: str, period_name: str):
    """Analyzes volatility using Plotly. This function is unchanged."""
    print("\n" + "="*80)
    print(f"--- Analyzing {period_name} Volatility ---")
    
    df_analysis = df.copy()
    df_analysis.set_index('time', inplace=True)
    df_analysis.sort_index(inplace=True)

    start_str, end_str = start_date or "start", end_date or "end"
    
    if start_date: df_analysis = df_analysis.loc[df_analysis.index >= start_date]
    if end_date: df_analysis = df_analysis.loc[df_analysis.index <= end_date]

    if df_analysis.empty:
        print(f"Error: No data in specified range ({start_str} to {end_str}).")
        print("="*80 + "\n"); return

    period_means = df_analysis.resample(period).mean()
    period_deltas = period_means.diff().dropna(how='all')

    if len(period_deltas) < 2:
        print(f"Not enough data for {period_name} analysis in this range.")
        print("="*80 + "\n"); return

    print(f"\n*** Stats of {period_name} *Changes* ({start_str} to {end_str}) ***\n")
    print(period_deltas.describe())

    if len(period_deltas) >= 10:
        print(f"\nGenerating interactive histogram for {period_name} changes...")
        params_to_plot = ['SVclockDrift', 'Crs', 'SVaccuracy']
        fig = make_subplots(
            rows=len(params_to_plot), cols=1,
            subplot_titles=[f'Distribution of {period_name} Change in {p}' for p in params_to_plot]
        )
        for i, param in enumerate(params_to_plot):
            if param in period_deltas.columns:
                fig.add_trace(go.Histogram(x=period_deltas[param].dropna(), name=param, nbinsx=50), row=i + 1, col=1)
        fig.update_layout(title_text=f'Interactive Distribution of {period_name} Changes ({start_str} to {end_str})', height=400 * len(params_to_plot), showlegend=False, template='plotly_white')
        for i, param in enumerate(params_to_plot):
             fig.update_yaxes(title_text='Frequency', row=i+1, col=1)
             fig.update_xaxes(title_text=f'Change in {param} per {period_name.rstrip("ly")}', row=i+1, col=1)
        output_filename = f'{period_name.lower()}_volatility_{start_str}_{end_str}.html'
        fig.write_html(output_filename)
        print(f"Saved interactive plot to '{output_filename}'")
    
    print("="*80 + "\n")

def get_date_from_user(prompt_text: str) -> str:
    """Gets and validates a date string from the user."""
    while True:
        date_str = input(prompt_text).strip()
        if not date_str: return None
        try: pd.to_datetime(date_str); return date_str
        except ValueError: print("Invalid format. Please use YYYY-MM-DD or press Enter.")

def main():
    """Main function with caching logic."""
    YOUR_RINEX_DIRECTORY = '/path/to/cddis_data_archive'
    cache_path = pathlib.Path(CACHE_FILENAME)
    root_path = pathlib.Path(YOUR_RINEX_DIRECTORY)
    
    # --- CACHING LOGIC ---
    if cache_path.is_file():
        # FAST PATH: Load from cache
        print(f"✅ Found cache file: '{CACHE_FILENAME}'")
        print("Loading pre-processed data in seconds...")
        ds = xr.open_dataset(cache_path)
        full_df = ds.to_dataframe().reset_index()
        print("Data loaded successfully from cache.")
    else:
        # SLOW PATH: Process RINEX and create cache
        print(f"❌ Cache file not found. Starting full RINEX processing.")
        print("This will take a long time on the first run.")
        if not root_path.is_dir():
            print(f"Error: Directory not found at '{YOUR_RINEX_DIRECTORY}'"); return

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

        if not all_
            print("\nError: No valid data could be extracted."); return

        print("\nAggregating all data into memory...")
        full_df = pd.concat(all_data, ignore_index=True)
        del all_data

        print(f"\n💾 Saving aggregated data to NetCDF cache: '{CACHE_FILENAME}'...")
        # Convert DataFrame to xarray Dataset and save to NetCDF
        xarray_dataset = full_df.to_xarray()
        xarray_dataset.to_netcdf(cache_path)
        print("Cache file created successfully.")

    # --- ANALYSIS PART (runs on data from either cache or RINEX) ---
    full_df['time'] = pd.to_datetime(full_df['time']) # Ensure time column is datetime
    
    print("\n" + "-"*50)
    print("Enter Date Range for Analysis")
    print("Format: YYYY-MM-DD. Press Enter for no limit.")
    start_date = get_date_from_user("Enter start date: ")
    end_date = get_date_from_user("Enter end date: ")
    print("-"*50)

    periods_to_analyze = {'D': 'Daily', 'W': 'Weekly', 'M': 'Monthly', 'Y': 'Yearly'}

    for code, name in periods_to_analyze.items():
        analyze_volatility(df=full_df, start_date=start_date, end_date=end_date, period=code, period_name=name)
    
    print("Analysis complete.")

if __name__ == '__main__':
    main()
