import pandas as pd
import xarray as xr
import os

# File paths
location_file = '/scratch/k10/ef7927/research_project/csv/all_stations/stations_coordinate.csv'
base_dir = '/g/data/rt52/era5/single-levels/reanalysis/tcwv/'
output_file = '/scratch/k10/ef7927/research_project/codes/tcwv/tcwv_all_days.nc'

# Load station locations
location_df = pd.read_csv(location_file, delimiter=';').rename(columns=str.strip)

# Validate required columns
if not {'latitude', 'longitude'}.issubset(location_df.columns):
    raise KeyError("Missing required columns: 'latitude' and/or 'longitude'.")

# Function to retrieve NetCDF files for a given year
def find_netcdf_files(year):
    year_dir = os.path.join(base_dir, str(year))
    if not os.path.exists(year_dir):
        print(f"No data directory found for year {year}.")
        return []
    return [os.path.join(root, file) for root, _, files in os.walk(year_dir) for file in files if file.endswith('.nc')]

# Extract TCWV data
tcwv_data = []
for year in range(2015, 2024):
    for netcdf_file in find_netcdf_files(year):
        with xr.open_dataset(netcdf_file) as ds:
            if 'tcwv' not in ds:
                print(f"'tcwv' variable missing in {netcdf_file}.")
                continue
            for _, loc in location_df.iterrows():
                tcwv = ds['tcwv'].sel(latitude=loc.latitude, longitude=loc.longitude, method='nearest')
                tcwv_data.extend({'time': t, 'latitude': loc.latitude, 'longitude': loc.longitude, 'tcwv': v} for t, v in zip(tcwv['time'].values, tcwv.values))

# Convert collected data to NetCDF
xr.Dataset.from_dataframe(pd.DataFrame(tcwv_data).set_index(['time', 'latitude', 'longitude'])).to_netcdf(output_file)
print(f"TCWV data saved to {output_file}.")