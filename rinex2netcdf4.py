import georinex as gr
from pathlib import Path
import logging
from tqdm import tqdm

# --- Configuration ---
# 1. Set the root directory of your data archive.
#    This should be the path to your "cddis_data_archive" folder.
ROOT_DIR = Path("cddis_data_archive")

# 2. Set the name for the output combined NetCDF file.
OUTPUT_FILE = "combined_gps_ephemeris.nc"

# 3. Specify the GNSS constellation(s) you want to include.
#    'G' for GPS, 'R' for GLONASS, 'E' for Galileo, etc.
#    For just GPS, use ["G"]. For multiple, use ["G", "R", "E"].
CONSTELLATIONS = ["G"] 

# --- Script ---

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def combine_rinex_nav(root_dir: Path, output_file: str, use_const: list):
    """
    Finds, combines, and saves RINEX navigation files to a NetCDF file.
    """
    if not root_dir.is_dir():
        logging.error(f"Root directory not found: {root_dir}")
        return

    # 1. Find all potential navigation files recursively.
    # The pattern '**.??[nN]*' is robust for RINEX 2 (.yyn) and RINEX 3 (.rnx, .yyN)
    # georinex handles compressed extensions like .Z, .gz, .zip automatically.
    logging.info(f"Searching for RINEX navigation files in: {root_dir}...")
    # We use rglob which is recursive. The pattern means:
    # **      -> any subdirectory
    # .??[nN] -> files ending in .<two_chars> followed by 'n' or 'N' (e.g., .23n, .23N)
    # *       -> any characters after that (to catch compression extensions)
    nav_files = sorted(list(root_dir.rglob("brdc*.*n*"))) # More specific pattern for IGS daily broadcast files
    # A more general pattern if you have other naming conventions:
    # nav_files = sorted(list(root_dir.rglob("*.??[nN]*")))
    
    if not nav_files:
        logging.warning("No navigation files found. Check your ROOT_DIR and file structure.")
        return

    logging.info(f"Found {len(nav_files)} potential navigation files. Preparing to load.")

    # 2. Load and combine the files.
    # georinex.load can take a list of files. It will read, parse, decompress,
    # and concatenate them into a single xarray.Dataset.
    # The `use=use_const` argument is crucial for filtering only the data you need (e.g., GPS).
    # This saves memory and processing time.
    try:
        logging.info(f"Loading and combining files for constellations: {use_const}...")
        # Using a list comprehension with tqdm for a progress bar
        # This is more memory-efficient for a very large number of files,
        # as it processes them one by one before concatenating.
        data_list = [gr.load(f, use=use_const) for f in tqdm(nav_files, desc="Processing files")]

        # Concatenate all the datasets in the list along the 'time' dimension
        # xarray is smart enough to merge the rest of the coordinates and variables.
        combined_nav = xr.concat(data_list, dim="time")

        # After concatenating, it's good practice to sort by time to ensure order.
        combined_nav = combined_nav.sortby('time')

    except Exception as e:
        logging.error(f"An error occurred during file loading: {e}")
        logging.error("This can happen with a corrupted file. Consider the 'Advanced' script version.")
        return

    logging.info("Successfully combined all navigation data.")
    print("\n--- Combined Data Summary ---")
    print(combined_nav)
    print("---------------------------\n")

    # 3. Save the combined data to a NetCDF4 file.
    # The 'encoding' helps with efficient storage of time variables.
    encoding = {
        v: {"zlib": True, "complevel": 5} for v in combined_nav.data_vars
    }
    logging.info(f"Saving combined data to: {output_file}")
    combined_nav.to_netcdf(output_file, engine="netcdf4", encoding=encoding)
    
    logging.info("Processing complete!")


if __name__ == "__main__":
    # We need to import xarray for the concatenation step
    try:
        import xarray as xr
    except ImportError:
        print("xarray is not installed. Please run: pip install xarray")
        exit()

    combine_rinex_nav(ROOT_DIR, OUTPUT_FILE, CONSTELLATIONS)
