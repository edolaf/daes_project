import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import numpy as np
import glob
import os
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# File paths
location_path = '/scratch/k10/ef7927/research_project/csv/all_stations/station_coordinate.csv'
base_dir = '/g/data/rt52/era5/pressure-levels/monthly-averaged/r/'
output_file='/scratch/k10/ef7927/research_project/netcdf/rh_all_days.nc

# Read CSV files
loaction = pd.read_csv(location_path)
## Clean column names (optional)
loaction.columns = loaction.columns.str.strip()
## Select only the 'latitude' and 'longitude' columns
location_df = loaction
location_df

# Clean column names
location_df.columns = location_df.columns.str.strip()

# Verify the column names
latitude_column = 'latitude'
longitude_column = 'longitude'

if latitude_column not in location_df.columns or longitude_column not in location_df.columns:
    raise KeyError("Columns '{}' and/or '{}' are missing in the CSV file.".format(latitude_column, longitude_column))

# Function to search for NetCDF files for a specific year
def find_netcdf_files_for_year(year):
    files = []
    year_dir = os.path.join(base_dir, str(year))
    
    if not os.path.exists(year_dir):
        print("No data directory found for year {}.".format(year))
        return files
    
    for root, dirs, files_in_year in os.walk(year_dir):
        for file in files_in_year:
            if file.endswith('.nc'):
                files.append(os.path.join(root, file))
    return files

# Initialize a dictionary to store the results for each location
rh_extreme_dict = {index: [] for index in location_df.index}

# Iterate over each year from 2015 to 2023
for year in range(2015, 2023 + 1):
    # Find NetCDF files for the current year
    netcdf_files = find_netcdf_files_for_year(year)

    if not netcdf_files:
        print("No NetCDF files found for year {}.".format(year))
        continue
    else:
        for netcdf_file in netcdf_files:
            # Open the NetCDF file
            ds = xr.open_dataset(netcdf_file)

            for index, location in location_df.iterrows():
                latitude = location[latitude_column]
                longitude = location[longitude_column]
                
                # Check if 't' variable exists in the dataset
                if 'r' not in ds:
                    print("'r' variable not found in {}.".format(netcdf_file))
                    continue
                
                # Select the 't' variable data for the specific location
                r = ds['r'].sel(latitude=latitude, longitude=longitude, method='nearest')
                
                # Store the selected data in the dictionary
                rh_extreme_dict[index].append(r)
            
            # Close the dataset
            ds.close()

# Compute the mean vertical profile for each location
rh_mean_profiles = {}

for index, rh_list in rh_extreme_dict.items():
    if rh_list:
        # Combine all the selected data into a single xarray DataArray
        rh_combined = xr.concat(rh_list, dim='time')
        
        # Calculate the time mean of the combined data
        rh_mean_profile = rh_combined.mean(dim='time')
        
        # Store the mean profile in the dictionary
        rh_mean_profiles[index] = rh_mean_profile

# Initialize an empty list to hold DataArrays
data_arrays = []

for index, rh_mean_profile in rh_mean_profiles.items():
    # Assuming `index` is an integer representing a specific location or station
    # We add it as a coordinate, but since it’s a single value, we’ll expand_dims accordingly
    rh_mean_profile = rh_mean_profile.expand_dims({'station': [index]})
    data_arrays.append(rh_mean_profile)

# Combine all DataArrays into a single Dataset
combined_dataset = xr.concat(data_arrays, dim='station')  # 'station' is the dimension for different locations

# Save the Dataset to a NetCDF file
combined_dataset.to_netcdf(output_file)