import os
import pandas as pd
import xarray as xr
import numpy as np

# File paths
csv_path = '/scratch/k10/ef7927/research_project/csv/all_stations/extreme_events.csv'
output_dir = '/scratch/k10/ef7927/research_project/netcdf/stations/999/'
os.makedirs(output_dir, exist_ok=True)

# Read the CSV file
df = pd.read_csv(csv_path)
 by location
# Initialize dictionary for temperature profiles by location
temp_profiles = {}

# Loop through each station in the CSV and process corresponding NetCDF files
for index, row in df.iterrows():
    date = pd.to_datetime(row['date'])
    lon, lat = row['longitude'], row['latitude']
    year, month = date.year, date.month
    days_in_month = pd.Period(f'{year}-{month:02d}').days_in_month

    # Construct NetCDF file path
    nc_file_name = f't_era5_oper_pl_{year}{month:02d}01-{year}{month:02d}{days_in_month:02d}.nc'
    nc_file_path = f'/g/data/rt52/era5/pressure-levels/reanalysis/t/{year}/{nc_file_name}'

    # Check if the file exists and process it
    if os.path.exists(nc_file_path):
        print(f"Processing {nc_file_name} (Row {index+1}/{len(df)})")
        with xr.open_dataset(nc_file_path) as ds:
            # Extract temperature data for the specific date, longitude, and latitude
            temperature_data = ds['t'].sel(time=date, longitude=lon, latitude=lat, method='nearest')

            # Store temperature profile in dictionary
            temp_profiles.setdefault((lon, lat), []).append(temperature_data)

    else:
        print(f"NetCDF file not found: {nc_file_path}")

# Save each location's temperature profile to a separate NetCDF file
for (lon, lat), temp_data_list in temp_profiles.items():
    # Concatenate temperature data along the time dimension
    temperature_array = xr.concat([data.drop_vars(['longitude', 'latitude']) for data in temp_data_list], dim='time')

    # Create and save the Dataset for this location
    ds_out = xr.Dataset(
        {'temperature': (['time', 'level'], temperature_array.values)},
        coords={'time': temperature_array.time, 'level': temperature_array.level, 'longitude': lon, 'latitude': lat}
    )
    nc_output_file = f'temperature_profile_lon{lon}_lat{lat}.nc'
    ds_out.to_netcdf(os.path.join(output_dir, nc_output_file))
    print(f"Saved temperature profile for {lon}, {lat} to {nc_output_file}")

# Step 5: Calculate the mean vertical profile for each location and combine all profiles
mean_profiles = {
    location: xr.concat(profiles, dim='time').mean(dim='time')
    for location, profiles in temp_profiles.items()
}

# Create a combined dataset for all locations
data_arrays = [profile.expand_dims({'station': [location]}) for location, profile in mean_profiles.items()]
combined_dataset = xr.concat(data_arrays, dim='station')

# Save the combined dataset with mean vertical temperature profiles
combined_dataset.to_netcdf('/scratch/k10/ef7927/research_project/netcdf/t_extreme_days.nc')
print("Combined dataset saved to t_extreme_days.nc")