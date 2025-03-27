import pandas as pd
import xarray as xr
import os

# Define file paths
csv_path = '/scratch/k10/ef7927/research_project/csv/all_stations/all_stations.csv'
output_dir = '/scratch/k10/ef7927/research_project/netcdf/stations/999/'

# Load the CSV file containing station information
df = pd.read_csv(csv_path)

# Initialize dictionary to store RH profiles by location
rh_profiles = {}

# Loop through each row in the DataFrame to process data for each station
for index, row in df.iterrows():
    date = pd.to_datetime(row['date'])
    lon, lat = row['longitude'], row['latitude']
    
    # Construct NetCDF file path
    nc_file_name = f'r_era5_oper_pl_{date.year}{date.month:02d}01-{date.year}{date.month:02d}{pd.Period(f"{date.year}-{date.month:02d}").days_in_month:02d}.nc'
    nc_file_path = f'/g/data/rt52/era5/pressure-levels/reanalysis/r/{date.year}/{nc_file_name}'

    # Check if the file exists
    if os.path.exists(nc_file_path):
        print(f"Processing: {nc_file_name} (Row: {index+1}/{len(df)})")

        # Open NetCDF file and select RH data for the specific time, lon, and lat
        ds = xr.open_dataset(nc_file_path)
        rh_data = ds['r'].sel(time=date, longitude=lon, latitude=lat, method='nearest')

        # Store RH data by location
        if (lon, lat) not in rh_profiles:
            rh_profiles[(lon, lat)] = []
        rh_profiles[(lon, lat)].append(rh_data)

        ds.close()  # Close the dataset to free resources
    else:
        print(f"NetCDF file not found: {nc_file_path}")

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Save RH profiles for each location
for (lon, lat), rh_data_list in rh_profiles.items():
    # Clean data and concatenate along the time dimension
    rh_data_cleaned = [rh_data.drop_vars(['longitude', 'latitude']) for rh_data in rh_data_list]
    rh_array = xr.concat(rh_data_cleaned, dim='time')

    # Create a new Dataset with longitude and latitude as coordinates
    ds_out = xr.Dataset(
        {'rh': (['time', 'level'], rh_array.values)},
        coords={'time': rh_array.time, 'level': rh_array.level, 'longitude': lon, 'latitude': lat}
    )

    # Save the dataset to a NetCDF file
    output_file = f'rh_profile_lon{lon}_lat{lat}.nc'
    ds_out.to_netcdf(os.path.join(output_dir, output_file))
    print(f"Saved RH profile to {output_file}")

# Step 6: Calculate the mean vertical profile for each location
mean_profiles = {location: xr.concat(profiles, dim='time').mean(dim='time') for location, profiles in rh_profiles.items()}

# Step 7: Display and process the mean profiles
for location, profile in mean_profiles.items():
    print(f"Location (lon, lat): {location}")
    print(profile)

# Step 8: Combine all mean profiles into a single Dataset and save
data_arrays = [profile.expand_dims({'station': [station]}) for station, profile in mean_profiles.items()]
combined_dataset = xr.concat(data_arrays, dim='station')

# Save the combined dataset
output_combined_file = '/scratch/k10/ef7927/research_project/netcdf/rh_extreme_days.nc'
combined_dataset.to_netcdf(output_combined_file)
print(f"Combined dataset saved to {output_combined_file}")