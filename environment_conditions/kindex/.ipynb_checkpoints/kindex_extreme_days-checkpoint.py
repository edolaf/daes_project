import os
import glob
import pandas as pd
import xarray as xr
import numpy as np
from metpy.calc import dewpoint_from_relative_humidity
from metpy.units import units
from calendar import monthrange
import matplotlib.pyplot as plt
import matplotlib.colors as colors

# Load station data
stations_df = pd.read_csv('/scratch/k10/ef7927/research_project/csv/all_stations/stations_coordinate.csv')

# Define pressure levels for K index calculation
pressure_levels = [500, 700, 850]

# Path to temperature and relative humidity data
temp_data_path = '/g/data/rt52/era5/pressure-levels/reanalysis/t/'
rh_data_path = '/g/data/rt52/era5/pressure-levels/reanalysis/r/'

# Function to get the correct NetCDF file path based on year and month
def get_netcdf_file(base_path, var, year, month):
    # Get the number of days in the month
    num_days = monthrange(year, month)[1]
    file_name = f"{var}_era5_oper_pl_{year}{month:02d}01-{year}{month:02d}{num_days}.nc"
    file_path = os.path.join(base_path, str(year), file_name)
    return file_path

# Calculate dew point temperature from temperature and relative humidity
def calculate_dew_point(T, RH):
    return T - ((100 - RH) / 5.0)

# Initialize an empty list to store results
k_index_results = []

# Loop through each station in the CSV
for _, station in stations_df.iterrows():
    lon, lat, date = station['longitude'], station['latitude'], pd.to_datetime(station['date'])
    year, month = date.year, date.month

    # Open the temperature and relative humidity NetCDF files for the respective year and month
    temp_file = get_netcdf_file(temp_data_path, 't', year, month)
    rh_file = get_netcdf_file(rh_data_path, 'r', year, month)
    
    try:
        with xr.open_dataset(temp_file) as temp_data, xr.open_dataset(rh_file) as rh_data:
            # Select the data for the given location, date, and pressure levels using nearest method for lon/lat
            temp_slice = temp_data.sel(longitude=lon, latitude=lat, time=date, level=pressure_levels, method='nearest')
            rh_slice = rh_data.sel(longitude=lon, latitude=lat, time=date, level=pressure_levels, method='nearest')

            # Convert temperature to Celsius
            T_500 = temp_slice.sel(level=500).t.values - 273.15  # 500 hPa temperature in Celsius
            T_700 = temp_slice.sel(level=700).t.values - 273.15  # 700 hPa temperature in Celsius
            T_850 = temp_slice.sel(level=850).t.values - 273.15  # 850 hPa temperature in Celsius

            # Extract relative humidity values
            RH_700 = rh_slice.sel(level=700).r.values
            RH_850 = rh_slice.sel(level=850).r.values

            # Calculate dew point temperatures at 700 hPa and 850 hPa
            TD_700 = calculate_dew_point(T_700, RH_700)
            TD_850 = calculate_dew_point(T_850, RH_850)

            # Calculate the K index
            K_index = (T_850 - T_500) + TD_850 - (T_700 - TD_700)

            # Append results to the list
            k_index_results.append({
                'date': date,
                'longitude': lon,
                'latitude': lat,
                'K_index': K_index
            })

    except FileNotFoundError:
        print(f"File not found for year {year}, month {month}. Skipping this station.")

# Convert the results to a DataFrame and save to CSV
k_index_df = pd.DataFrame(k_index_results)
output_path = '/scratch/k10/ef7927/research_project/codes/kindex/kindex_extreme_days.csv'
k_index_df.to_csv(output_path, index=False)

print(f"K Index calculation completed. Results saved to {output_path}")