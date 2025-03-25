import pandas as pd
import xarray as xr
import os

# File paths
extreme_events_path = "/scratch/k10/ef7927/research_project/csv/all_stations/extreme_events.csv"
location_path = "/scratch/k10/ef7927/research_project/csv/all_stations/station_coordinate.csv"
output_file = "/scratch/k10/ef7927/research_project/netcdf/cape_all_days.nc"
base_dir = "/g/data/rt52/era5/single-levels/reanalysis/cape/"


# Read CSV files
extreme_events_df = pd.read_csv(extreme_events_path)
location_df = pd.read_csv(location_path)

# Clean column names
location_df.columns = location_df.columns.str.strip()

# Ensure required columns exist
if "latitude" not in location_df or "longitude" not in location_df:
    raise KeyError("Missing 'latitude' and/or 'longitude' in location_df.")

# Initialize a list to store DataArrays for each year's data
data_arrays = []

# Process NetCDF files for each year
for year in range(2015, 2024):
    year_dir = os.path.join(base_dir, str(year))

    if not os.path.exists(year_dir):
        print(f"No data directory found for year {year}. Skipping...")
        continue

    for root, _, files in os.walk(year_dir):
        netcdf_files = [os.path.join(root, f) for f in files if f.endswith('.nc')]
        
        if not netcdf_files:
            print(f"No NetCDF files found for {year}.")
            continue

        for netcdf_file in netcdf_files:
            with xr.open_dataset(netcdf_file) as ds:
                if "cape" not in ds:
                    print(f"'cape' variable missing in {netcdf_file}. Skipping...")
                    continue

                # Select closest grid points for all locations at once
                cape_selected = ds["cape"].sel(
                    latitude=xr.DataArray(location_df["latitude"], dims="points"),
                    longitude=xr.DataArray(location_df["longitude"], dims="points"),
                    method="nearest"
                )

                # Reshape DataArray to DataFrame
                cape_df = cape_selected.to_dataframe().reset_index()
                data_arrays.append(cape_df)

# Combine all collected data
if data_arrays:
    cape_combined = pd.concat(data_arrays, ignore_index=True)
    cape_xr = cape_combined.set_index(["time", "latitude", "longitude"]).to_xarray()
    cape_xr.to_netcdf(output_file)
    print(f"CAPE data saved to {output_file}.")
else:
    print("No CAPE data found for any year.")