import pathlib
import pandas as pd
import georinex as gr
from tqdm import tqdm
import warnings
import multiprocessing

# --- 1. Worker Function ---
# This function will be executed by each parallel process.
# It must be defined at the top level of the script.

# Suppress warnings from georinex about file versions, which are common with old files.
warnings.filterwarnings("ignore", category=UserWarning)

# Define the parameters globally so they don't need to be passed to each process
PARAMS_TO_ANALYZE = [
    'SVclockBias', 'SVclockDrift', 'SVclockDriftRate', 'IODE', 'Crs',
    'Delta_n', 'M0', 'Cuc', 'Eccentricity', 'Cus', 'sqrtA', 'Toe',
    'Cic', 'Omega0', 'Cis', 'Io', 'Crc', 'omega', 'OmegaDot',
    'IDOT', 'TGD', 'SVaccuracy', 'health'
]

def process_rinex_file(file_path: pathlib.Path):
    """
    Processes a single RINEX navigation file. This function is designed
    to be called by a multiprocessing Pool.

    Args:
        file_path: The path to the RINEX file.

    Returns:
        A tuple: ('success', pandas.DataFrame) on success, or 
                 ('error', (str, str)) with the file path and error message on failure.
    """
    try:
        # Load the navigation file
        nav_data = gr.load(file_path)
        
        # Convert to a pandas DataFrame
        df = nav_data.to_dataframe(fast=True).reset_index()

        # Ensure we only process GPS ('G') data
        if 'constellation' in df.columns:
            df = df[df['constellation'] == 'G']

        # Drop rows where essential parameters are missing
        df.dropna(subset=PARAMS_TO_ANALYZE, inplace=True)

        if not df.empty:
            return ('success', df[PARAMS_TO_ANALYZE])
        else:
            # File was valid but contained no usable GPS data
            return ('nodata', str(file_path))

    except Exception as e:
        # Return the error message and file path for logging
        return ('error', (str(file_path.name), str(e)))

# --- 2. Main Analysis Script ---

def analyze_gps_ephemeris_parallel(rinex_root_dir: str):
    """
    Analyzes 30+ years of GPS ephemeris data in parallel.

    Args:
        rinex_root_dir: The path to the top-level directory containing the RINEX files.
    """
    root_path = pathlib.Path(rinex_root_dir)
    if not root_path.is_dir():
        print(f"Error: Directory not found at '{rinex_root_dir}'")
        return

    print("Searching for RINEX navigation files...")
    nav_files = list(root_path.rglob('*.nav')) + list(root_path.rglob('*.*n'))
    nav_files = sorted(list(set(nav_files)))
    
    if not nav_files:
        print(f"Error: No RINEX navigation files found in '{rinex_root_dir}'.")
        return
        
    print(f"Found {len(nav_files)} potential navigation files.")

    # --- Data Extraction (Parallelized) ---
    all_data = []
    failed_files = []
    
    print(f"\n--- Parsing files in parallel using {multiprocessing.cpu_count()} CPU cores ---")
    
    # The 'with' statement ensures the pool is properly closed
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        # 'imap_unordered' is efficient as it processes results as they complete
        # 'tqdm' provides the progress bar
        results_iterator = pool.imap_unordered(process_rinex_file, nav_files)
        
        for result in tqdm(results_iterator, total=len(nav_files), desc="Processing files"):
            status, data = result
            if status == 'success':
                all_data.append(data)
            elif status == 'error':
                failed_files.append(data)

    if failed_files:
        print(f"\n--- Encountered {len(failed_files)} errors during processing ---")
        for i, (filename, error) in enumerate(failed_files):
            if i < 10: # Print first 10 errors
                print(f"  - {filename}: {error}")
        if len(failed_files) > 10:
            print(f"  ... and {len(failed_files) - 10} more.")

    if not all_
        print("\nError: No valid data could be extracted from the files.")
        return

    # --- Data Aggregation ---
    print("\n--- Aggregating data from all files ---")
    print("This step is single-threaded and can be very memory-intensive...")
    full_df = pd.concat(all_data, ignore_index=True)
    
    del all_data # Free up memory
    
    print(f"Successfully aggregated {len(full_df)} ephemeris records.")
    
    # --- Statistical Analysis (unchanged) ---
    print("\n--- Performing Statistical Analysis ---")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 120)
    
    print("\n*** Basic Descriptive Statistics ***")
    print(full_df.describe())
    
    print("\n*** Variance ***")
    print(full_df.var())

    print("\n*** Skewness & Kurtosis (Measures of Distribution Shape) ***")
    stats_df = pd.DataFrame({
        'skewness': full_df.skew(),
        'kurtosis': full_df.kurt()
    })
    print(stats_df)
    
    print("\n*** Correlation Matrix ***")
    print("Calculating correlation matrix...")
    correlation_matrix = full_df.corr()
    print(correlation_matrix)
    # correlation_matrix.to_csv('gps_ephemeris_correlation.csv')
    # print("\nCorrelation matrix saved to 'gps_ephemeris_correlation.csv'")


# --- Script Execution ---
if __name__ == '__main__':
    # This check is CRUCIAL for multiprocessing. It prevents child processes
    # from re-executing the script's code, which would lead to an infinite
    # loop of process creation.
    
    # !!! IMPORTANT !!!
    # Replace this with the actual path to your Rinex files.
    YOUR_RINEX_DIRECTORY = '/path/to/your/rinex/files'
    
    analyze_gps_ephemeris_parallel(YOUR_RINEX_DIRECTORY)
