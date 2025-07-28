import matplotlib.pyplot as plt
import math
import numpy as np
import scipy as sp
import georinex as gr
import os

rinex_path = "./rinex_files/brdc1920.25g"

def main():
    eph = gr.load(rinex_path)

    print("Header information:")
    print(eph.attrs)

    # Print the observation data
    print("Observation data:")
    print(eph)

if __name__=="__main__":
    main()
