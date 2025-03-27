import pandas as pd
import xarray as xr
import os
from dask import delayed, compute
from dask.distributed import Client

# Start Dask client (this will utilize the cluster)
client = Client()

# File paths and base directory for NetCDF files
location_path = '/scratch/k10/ef7927/research_project/csv/all_stations/station_coordinate.csv'
base_dir = '/g/data/rt52/era5/pressure-levels/reanalysis/t/'

# Read CSV and clean the location data
location_df = pd.read_csv(location_path).apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# Verify that latitude and longitude columns exist
latitude_column, longitude_column = 'latitude', 'longitude'
if latitude_column not in location_df or longitude_column not in location_df:
    raise KeyError(f"Columns '{latitude_column}' and/or '{longitude_column}' are missing.")

# Define the pressure levels to select
pressure_levels = [100, 200, 300, 500, 700, 850, 925, 1000]

# Function to find NetCDF files for a given year
def find_netcdf_files(year):
    year_dir = os.path.join(base_dir, str(year))
    return [os.path.join(root, file) for root, _, files in os.walk(year_dir) for file in files if file.endswith('.nc')] if os.path.exists(year_dir) else []

# Initialize dictionary to store temperature data
t_extreme_dict = {index: [] for index in location_df.index}

# Function to process each NetCDF file with Dask
@delayed
def process_file(netcdf_file):
    with xr.open_dataset(netcdf_file) as ds:
        for index, location in location_df.iterrows():
            latitude, longitude = location[latitude_column], location[longitude_column]
            if 't' in ds:
                try:
                    t = ds['t'].sel(latitude=latitude, longitude=longitude, method='nearest')
                    if 'level' in t.dims:
                        t = t.sel(level=pressure_levels)
                    t_extreme_dict[index].append(t)
                except Exception as e:
                    print(f"Error at index {index} for location ({latitude}, {longitude}): {e}")

# Generate Dask tasks for each NetCDF file in the specified years
tasks = [process_file(file) for year in range(2015, 2024) for file in find_netcdf_files(year)]

# Execute tasks in parallel using Dask
compute(*tasks)

# Compute the mean vertical profile for each location
t_mean_profiles = {
    index: xr.concat(t_list, dim='time').mean(dim='time') for index, t_list in t_extreme_dict.items() if t_list
}

# Combine all mean profiles into a single Dataset
data_arrays = [profile.expand_dims({'station': [index]}) for index, profile in t_mean_profiles.items()]
combined_dataset = xr.concat(data_arrays, dim='station')

# Save the combined dataset to a NetCDF file
combined_dataset.to_netcdf('/scratch/k10/ef7927/research_project/netcdf/t_all_days.nc')