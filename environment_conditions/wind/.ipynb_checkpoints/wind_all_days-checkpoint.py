import os
import xarray as xr
import numpy as np
import calendar

# Define directories and years
u_dir = '/g/data/rt52/era5/pressure-levels/reanalysis/u/'
v_dir = '/g/data/rt52/era5/pressure-levels/reanalysis/v/'
years = range(2015, 2024)

# Define latitude and longitude ranges for slicing
lat_range = slice(5.0, -5.0)  # This remains unchanged
lon_range = slice(95.0, 105.0)  # This remains unchanged

# Function to load and slice data
def load_and_slice(file_path, time_range, lat_range, lon_range):
    ds = xr.open_dataset(file_path)
    print(f"Loaded {file_path} with dimensions: {ds.dims}")  # Debugging output
    print(f"Available coordinates: {ds.coords}")  # Debugging output
    print(f"Latitude values: {ds['latitude'].values}")  # Debugging output
    
    # Slicing only the time dimension
    ds_sliced = ds.sel(time=time_range)
    print(f"Sliced {file_path} to dimensions (before geographical slicing): {ds_sliced.dims}")  # Debugging output

    # Slicing for latitude and longitude
    ds_sliced_geo = ds_sliced.sel(latitude=lat_range, longitude=lon_range)
    print(f"Sliced {file_path} to dimensions (after geographical slicing): {ds_sliced_geo.dims}")  # Debugging output
    return ds_sliced_geo

# Initialize lists to hold data
u_data_list = []
v_data_list = []

# Loop through years and months
for year in years:
    for month in range(1, 13):
        # Get the last day of the month
        last_day = calendar.monthrange(year, month)[1]  # Gives the correct number of days for the month
        
        # Define file pattern based on month and year
        u_file = '{}/u_era5_oper_pl_{}{:02d}01-{}{:02d}{:02d}.nc'.format(u_dir + str(year), year, month, year, month, last_day)
        v_file = '{}/v_era5_oper_pl_{}{:02d}01-{}{:02d}{:02d}.nc'.format(v_dir + str(year), year, month, year, month, last_day)
        
        # Load and slice u component
        if os.path.exists(u_file):
            u_data_list.append(load_and_slice(u_file, slice(None), lat_range, lon_range))  # Keep all time data
        
        # Load and slice v component
        if os.path.exists(v_file):
            v_data_list.append(load_and_slice(v_file, slice(None), lat_range, lon_range))  # Keep all time data

# Concatenate data along time dimension
u_combined = xr.concat(u_data_list, dim='time')
v_combined = xr.concat(v_data_list, dim='time')

# Step 2: Define the target pressure levels
target_levels = [100, 200, 300, 500, 700, 850, 925, 1000]

# Step 3: Select the U and V components for the specific levels and time-average them
u_selected = u_combined['u'].sel(level=target_levels).mean(dim='time')
v_selected = v_combined['v'].sel(level=target_levels).mean(dim='time')

# Step 4: Calculate Wind Speed and Wind Direction for the time-averaged data
wind_speed = np.sqrt(u_selected**2 + v_selected**2)  # Wind speed
wind_direction = (270 - np.rad2deg(np.arctan2(v_selected, u_selected))) % 360  # Wind direction

# Print debug info to confirm calculations
print(f"Target levels: {target_levels}")
print(f"Wind Speed shape: {wind_speed.shape}, sample values:\n{wind_speed.isel(level=0).values[:5, :5]}")
print(f"Wind Direction shape: {wind_direction.shape}, sample values:\n{wind_direction.isel(level=0).values[:5, :5]}")

# Step 5: Save the time-averaged wind speed and direction to a new NetCDF file
output_path = f'/scratch/k10/ef7927/research_project/codes/wind/wind_time_avg_levels_all_days.nc'

ds_out = xr.Dataset(
    {
        'wind_speed': (['level', 'latitude', 'longitude'], wind_speed.data),
        'wind_direction': (['level', 'latitude', 'longitude'], wind_direction.data),
    },
    coords={
        'latitude': u_selected.latitude,
        'longitude': u_selected.longitude,
        'level': target_levels
    }
)

# Add attributes to the variables
ds_out['wind_speed'].attrs['units'] = 'm/s'
ds_out['wind_direction'].attrs['units'] = 'degrees'
ds_out['wind_speed'].attrs['description'] = 'Time-averaged wind speed'
ds_out['wind_direction'].attrs['description'] = 'Time-averaged wind direction'

# Save to NetCDF
ds_out.to_netcdf(output_path)
print(f"Time-averaged wind data saved to {output_path}")

# Close the datasets to free up resources
u_combined.close()
v_combined.close()