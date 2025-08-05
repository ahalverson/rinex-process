import socket
import subprocess
import os
from datetime import datetime
import time

# --- Configuration ---
# Receiver connection details
RECEIVER_IP = "192.168.3.1"
RECEIVER_PORT = 28785 # Use the port for the 'IP Connection' (TCPServer) stream

# Path to the sbf2rin executable
SBF2RIN_PATH = "/opt/Septentrio/RxTools/bin/sbf2rin"

# --- Streaming Architecture Settings ---
# How often to process data and update the RINEX file (in seconds)
PROCESS_INTERVAL_SECONDS = 60

# Directory to store the master SBF log and the output RINEX file
OUTPUT_DIR = "rinex_stream_output"

# The final, continuously updated RINEX navigation file
# Using a fixed name makes it easy for other programs to read
LIVE_RINEX_FILENAME = "live_nav.rnx"

# RINEX settings
RINEX_VERSION = "211"
RINEX_MARKER_NAME = "AFRL"

def convert_sbf_to_rinex(sbf_filepath, output_rinex_path):
    """
    Calls sbf2rin to convert the master SBF file, overwriting a fixed RINEX file.
    """
    print(f"-> Converting {sbf_filepath} to {output_rinex_path}")
    
    # Use the -O flag to specify the exact output filename and force overwrite.
    # Use the -n flag to generate ONLY navigation files.
    command = [
        SBF2RIN_PATH,
        "-f", sbf_filepath,
        f"-R{RINEX_VERSION}",
        "-nN",
        "-O", output_rinex_path # CRITICAL: Specify exact output path
    ]
    
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=30 # Add a timeout to prevent hangs
        )
        print(f"   Conversion successful. RINEX file '{LIVE_RINEX_FILENAME}' is up to date.")
        # Optional: print sbf2rin output for verbosity
        # if result.stdout:
        #     print(result.stdout)
            
    except FileNotFoundError:
        print(f"Error: '{SBF2RIN_PATH}' not found. Please check configuration.")
        return False
    except subprocess.CalledProcessError as e:
        print(f"Error during conversion. sbf2rin returned non-zero exit status {e.returncode}.")
        print(f"   sbf2rin stderr: {e.stderr}")
        return False
    except subprocess.TimeoutExpired:
        print("Error: sbf2rin conversion timed out.")
        return False
        
    return True

def main():
    """
    Main loop to continuously capture SBF data, append it to a log,
    and convert it to an updated RINEX navigation file.
    """
    print("--- SBF to RINEX Streaming Service Starting ---")
    
    # Create the output directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")

    # Define paths for our master log and final RINEX file
    master_sbf_filename = f"master_log_{datetime.utcnow().strftime('%Y%m%d')}.sbf"
    master_sbf_filepath = os.path.join(OUTPUT_DIR, master_sbf_filename)
    live_rinex_filepath = os.path.join(OUTPUT_DIR, LIVE_RINEX_FILENAME)
    
    print(f"Master SBF log: {master_sbf_filepath}")
    print(f"Live RINEX output: {live_rinex_filepath}")
    print(f"Processing interval: {PROCESS_INTERVAL_SECONDS} seconds")

    while True:
        try:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Connecting to receiver...")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((RECEIVER_IP, RECEIVER_PORT))
                print("Connection successful. Capturing data...")
                
                # Append new data to the master log file
                with open(master_sbf_filepath, 'ab') as sbf_file:
                    start_time = time.time()
                    bytes_received = 0
                    while time.time() - start_time < PROCESS_INTERVAL_SECONDS:
                        data = sock.recv(4096)
                        if not 
                            print("Connection closed by receiver.")
                            break
                        sbf_file.write(data)
                        bytes_received += len(data)

                print(f"Captured {bytes_received} bytes in this interval.")

            # After capturing a chunk, re-process the entire master file
            if bytes_received > 0:
                convert_sbf_to_rinex(master_sbf_filepath, live_rinex_filepath)
            else:
                print("No data received in this interval. Skipping conversion.")

        except socket.error as e:
            print(f"Socket Error: {e}. Retrying in 10 seconds...")
            time.sleep(10)
        except Exception as e:
            print(f"An unexpected error occurred: {e}. Retrying in 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n--- Service stopped by user. ---")
