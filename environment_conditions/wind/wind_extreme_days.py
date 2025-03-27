import os
import pandas as pd
import xarray as xr
import calendar
import numpy as np

# Step 1: Read the CSV file
csv_path = '/scratch/k10/ef7927/research_project/csv/all_stations/stations_coordinate.csv'
df = pd.read_csv(csv_path)

# Define latitude and longitude ranges for slicing
lat_range = slice(5.0, -5.0)  # Adjusted latitude range
lon_range = slice(95.0, 105.0)  # Adjusted longitude range

# Define the target pressure levels
target_levels = [100, 200, 300, 500, 700, 850, 925, 1000]

# Initialize lists to store U and V wind components
u_wind_profiles = []
v_wind_profiles = []

# Step 2: Loop through each row in the DataFrame
for index, row in df.iterrows():
    date_str = row['date']  # Extract the date from the CSV
  
    # Convert the date to a datetime object
    date = pd.to_datetime(date_str)
    
    # Get the year and month
    year = date.year
    month = date.month
    
    # Get the number of days in the month
    days_in_month = calendar.monthrange(year, month)[1]
    
    # Construct the U and V component NetCDF file names based on the year and month
    u_file_name = 'u_era5_oper_pl_{}{:02d}01-{}{:02d}{:02d}.nc'.format(year, month, year, month, days_in_month)
    v_file_name = 'v_era5_oper_pl_{}{:02d}01-{}{:02d}{:02d}.nc'.format(year, month, year, month, days_in_month)
    
    # Construct the full paths to the NetCDF files
    u_file_path = '/g/data/rt52/era5/pressure-levels/reanalysis/u/{}/{}'.format(year, u_file_name)
    v_file_path = '/g/data/rt52/era5/pressure-levels/reanalysis/v/{}/{}'.format(year, v_file_name)

    # Step 3: Check if the U component NetCDF file exists
    if os.path.exists(u_file_path):
        print(f"Processing U file: {u_file_name} (Row: {index+1}/{len(df)})")
        
        # Open the U component dataset
        ds_u = xr.open_dataset(u_file_path)
        
        # Step 3.1: Slice the dataset by latitude and longitude
        u_data = ds_u['u'].sel(latitude=lat_range, longitude=lon_range)
        
        # Step 3.2: Select the time using the nearest method
        u_data_nearest_time = u_data.sel(time=date, method='nearest')
        
        # Append the sliced data to the list
        u_wind_profiles.append(u_data_nearest_time)
        
        # Close the U dataset to free resources
        ds_u.close()
    else:
        print(f"U NetCDF file not found: {u_file_path}")
    
    # Step 4: Check if the V component NetCDF file exists
    if os.path.exists(v_file_path):
        print(f"Processing V file: {v_file_name} (Row: {index+1}/{len(df)})")
        
        # Open the V component dataset
        ds_v = xr.open_dataset(v_file_path)
        
        # Step 4.1: Slice the dataset by latitude and longitude
        v_data = ds_v['v'].sel(latitude=lat_range, longitude=lon_range)
        
        # Step 4.2: Select the time using the nearest method
        v_data_nearest_time = v_data.sel(time=date, method='nearest')
        
        # Append the sliced data to the list
        v_wind_profiles.append(v_data_nearest_time)
        
        # Close the V dataset to free resources
        ds_v.close()
    else:
        print(f"V NetCDF file not found: {v_file_path}")

# Step 5: Concatenate and process the U and V wind data for the entire region over time
if u_wind_profiles and v_wind_profiles:
    u_combined = xr.concat(u_wind_profiles, dim='time')
    v_combined = xr.concat(v_wind_profiles, dim='time')

    # Step 6: Select the U and V components for the specific pressure levels and time-average them
    u_selected = u_combined.sel(level=target_levels).mean(dim='time')
    v_selected = v_combined.sel(level=target_levels).mean(dim='time')

    # Step 7: Calculate Wind Speed and Wind Direction for the time-averaged data
    wind_speed = np.sqrt(u_selected**2 + v_selected**2)  # Wind speed
    wind_direction = (270 - np.rad2deg(np.arctan2(v_selected, u_selected))) % 360  # Wind direction

    # Print debug info to confirm calculations
    print(f"Target levels: {target_levels}")
    print(f"Wind Speed shape: {wind_speed.shape}, sample values:\n{wind_speed.isel(level=0).values[:5, :5]}")
    print(f"Wind Direction shape: {wind_direction.shape}, sample values:\n{wind_direction.isel(level=0).values[:5, :5]}")

    # Step 8: Save the time-averaged wind speed and direction to a new NetCDF file
    output_path = f'/scratch/k10/ef7927/research_project/codes/wind/wind_time_avg_levels_extreme_days.nc'

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
else:
    print("No valid U and V data found for processing.")