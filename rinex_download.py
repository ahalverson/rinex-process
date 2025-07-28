import requests
import os
from datetime import datetime, timedelta
import calendar # To check for leap years

def is_leap_year(year):
    """Checks if a given year is a leap year."""
    return calendar.isleap(year)

def download_cddis_rinex_v2_gps_nav_bulk(start_year, end_year, base_output_directory="cddis_data_archive"):
    """
    Downloads daily merged GPS broadcast ephemeris files (RINEX v2) from NASA CDDIS
    for a range of years, maintaining the CDDIS folder structure.

    Args:
        start_year (int): The starting four-digit year (e.g., 2000).
        end_year (int): The ending four-digit year (e.g., 2025).
        base_output_directory (str): The top-level local directory to save the data.
                                     Defaults to "cddis_data_archive".
    """

    base_cddis_url = "https://cddis.nasa.gov/archive/gnss/data/daily/"

    # Create the base output directory if it doesn't exist
    os.makedirs(base_output_directory, exist_ok=True)
    print(f"All files will be saved under: {os.path.abspath(base_output_directory)}")

    session = requests.Session()
    # The requests library will automatically look for .netrc by default
    # Ensure your .netrc is configured correctly for Earthdata Login.

    for year in range(start_year, end_year + 1):
        num_days_in_year = 366 if is_leap_year(year) else 365
        print(f"\n--- Starting download for year: {year} ({num_days_in_year} days) ---")

        for day_of_year in range(1, num_days_in_year + 1):
            current_date = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
            two_digit_year = current_date.strftime('%y') # e.g., '00' for 2000, '25' for 2025
            three_digit_day = f"{day_of_year:03d}" # e.g., '001', '010', '365'

            # --- RINEX v2 Daily GPS Navigation Data File Pattern ---
            # CDDIS Path Structure: YYYY/DDD/YYn/
            # Filename: brdcDDD0.YYn.gz (or .Z for older files, handled below)
            
            file_type_subdir = f"{two_digit_year}n" # 'n' for GPS navigation data

            # Construct the CDDIS URL path
            cddis_directory_path = f"{year}/{three_digit_day}/{file_type_subdir}/"
            base_file_name_prefix = f"brdc{three_digit_day}0.{two_digit_year}n"

            # Local path to mirror CDDIS structure
            local_subdirectory = os.path.join(base_output_directory, str(year), three_digit_day, file_type_subdir)
            os.makedirs(local_subdirectory, exist_ok=True)

            # Check for both .gz and .Z extensions, preferring .gz for newer files
            possible_extensions = ['.gz', '.Z']
            downloaded = False

            for ext in possible_extensions:
                filename = base_file_name_prefix + ext
                file_url = f"{base_cddis_url}{cddis_directory_path}{filename}"
                local_filepath = os.path.join(local_subdirectory, filename)

                print(f"Attempting to download: {file_url}")

                try:
                    with session.get(file_url, stream=True) as response:
                        # Check if the file exists and is accessible (status code 200)
                        if response.status_code == 200:
                            # Check for redirection to Earthdata Login page
                            if "urs.earthdata.nasa.gov" in response.url:
                                print(f"Authentication failed or redirected to Earthdata Login for {file_url}.")
                                print("Please ensure your .netrc file is correctly configured.")
                                print("Skipping this file and remaining files for this session.")
                                # Consider breaking the outer loops here if authentication fails completely
                                return # Exit function if authentication is the core issue
                            
                            total_size = int(response.headers.get('content-length', 0))
                            downloaded_size = 0

                            with open(local_filepath, 'wb') as f:
                                for chunk in response.iter_content(chunk_size=8192):
                                    if chunk:
                                        f.write(chunk)
                                        downloaded_size += len(chunk)
                            print(f"Successfully downloaded {filename} to {local_filepath}")
                            downloaded = True
                            break # Move to the next day if successfully downloaded
                        elif response.status_code == 404:
                            print(f"File not found (404) at {file_url}. Trying next extension if available.")
                            # Continue to try the next extension if 404
                        else:
                            response.raise_for_status() # Raise for other 4xx/5xx errors
                except requests.exceptions.RequestException as e:
                    print(f"Error downloading {filename} from {file_url}: {e}")
                except Exception as e:
                    print(f"An unexpected error occurred for {filename} from {file_url}: {e}")
            
            if not downloaded:
                print(f"Could not find or download {base_file_name_prefix}.gz or .Z for Day {three_digit_day}, Year {year}.")
                print("This might indicate the file doesn't exist on CDDIS for this day or year,")
                print("or there's an issue with the file naming convention for this specific period.")


if __name__ == "__main__":
    print("CDDIS Daily GPS Navigation Data (RINEX v2) Bulk Downloader")
    print("---------------------------------------------------------")
    print("NOTE: You MUST have an Earthdata Login account and a correctly configured .netrc file in your home directory.")
    print("Example .netrc content:")
    print("  machine urs.earthdata.nasa.gov login <your_username> password <your_password>")
    print("Ensure file permissions are set to read-only for your user (e.g., chmod 600 ~/.netrc on Linux/macOS).")

    start_year_input = 2000
    end_year_input = 2025 # As requested

    # You can uncomment these lines if you want to prompt for years
    # while True:
    #     try:
    #         start_year_input = int(input("Enter the starting year (e.g., 2000): "))
    #         if start_year_input < 1980 or start_year_input > datetime.now().year:
    #             print("Please enter a realistic start year.")
    #             continue
    #         break
    #     except ValueError:
    #         print("Invalid input. Please enter a number.")
    
    # while True:
    #     try:
    #         end_year_input = int(input("Enter the ending year (e.g., 2025): "))
    #         if end_year_input < start_year_input or end_year_input > datetime.now().year + 1:
    #             print("End year must be >= start year and realistic.")
    #             continue
    #         break
    #     except ValueError:
    #         print("Invalid input. Please enter a number.")

    base_output_dir = input("Enter a base directory to save all data (leave blank for 'cddis_data_archive'): ")
    if not base_output_dir:
        base_output_dir = "cddis_data_archive"

    download_cddis_rinex_v2_gps_nav_bulk(start_year_input, end_year_input, base_output_dir)
    print("\nBulk download process complete.")
