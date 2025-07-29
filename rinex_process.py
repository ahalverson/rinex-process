import pathlib
import pandas as pd
import georinex as gr
from tqdm import tqdm
import warnings
import multiprocessing

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
    Processes a single RINEX navigation file (compressed or uncompressed).
    This function remains UNCHANGED as georinex handles compression automatically.
    """
    try:
        # georinex will handle .Z, .gz, etc. automatically here.
        nav_data = gr.load(file_path)
        
        df = nav_data.to_dataframe(fast=True).reset_index()

        if 'constellation' in df.columns:
            df = df[df['constellation'] == 'G']

        df.dropna(subset=PARAMS_TO_ANALYZE, inplace=True)

        if not df.empty:
            return ('success', df[PARAMS_TO_ANALYZE])
        else:
            return ('nodata', str(file_path))

    except Exception as e:
        return ('error', (str(file_path.name), str(e)))


def analyze_gps_ephemeris_parallel(rinex_root_dir: str):
    """
    Analyzes 30+ years of GPS ephemeris data in parallel from a
    CDDIS-like archive structure.
    """
    root_path = pathlib.Path(rinex_root_dir)
    if not root_path.is_dir():
        print(f"Error: Directory not found at '{rinex_root_dir}'")
        return

    # --- MODIFIED SECTION: Advanced File Searching ---
    print("Searching for RINEX navigation files (including .Z and .gz compressed files)...")
    
    # This pattern is now robust enough for typical archive structures.
    # It finds RINEX 2 (.YYn) and RINEX 3 (.nav) files, compressed or not.
    # The 'rglob' function searches recursively through your specified structure.
    print(" -> Searching for .gz files...")
    nav_files_gz = list(root_path.rglob('*.*n.gz')) + list(root_path.rglob('*.nav.gz'))
    
    print(" -> Searching for .Z files (common in CDDIS)...")
    nav_files_Z = list(root_path.rglob('*.*n.Z')) + list(root_path.rglob('*.nav.Z'))
    
    print(" -> Searching for uncompressed files...")
    nav_files_uncompressed = list(root_path.rglob('*.*n')) + list(root_path.rglob('*.nav'))
    
    # Combine all lists and remove duplicates
    all_found_files = nav_files_gz + nav_files_Z + nav_files_uncompressed
    nav_files = sorted(list(set(all_found_files)))
    # --- END OF MODIFIED SECTION ---

    if not nav_files:
        print(f"\nError: No RINEX navigation files found in '{rinex_root_dir}'.")
        print("Please check the path and that files match patterns like '*.23n.Z', '*.nav.gz', etc.")
        return
        
    print(f"\nFound {len(nav_files)} potential navigation files.")

    all_data = []
    failed_files = []
    
    print(f"\n--- Parsing files in parallel using {multiprocessing.cpu_count()} CPU cores ---")
    
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
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
            if i < 10:
                print(f"  - {filename}: {error}")
        if len(failed_files) > 10:
            print(f"  ... and {len(failed_files) - 10} more.")

    if not all_
        print("\nError: No valid data could be extracted from the files.")
        return

    print("\n--- Aggregating data from all files ---")
    print("This step is single-threaded and can be very memory-intensive...")
    full_df = pd.concat(all_data, ignore_index=True)
    
    del all_data
    
    print(f"Successfully aggregated {len(full_df)} ephemeris records.")
    
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


if __name__ == '__main__':
    # This guard is essential for multiprocessing to work correctly.
    
    # !!! IMPORTANT !!!
    # Set this to the top-level directory of your archive.
    # The script will search through all subdirectories like /2023/123/ etc.
    YOUR_RINEX_DIRECTORY = '/path/to/cddis_data_archive'
    
    analyze_gps_ephemeris_parallel(YOUR_RINEX_DIRECTORY)
