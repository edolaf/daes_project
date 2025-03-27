import pandas as pd
import xarray as xr
import os
import glob

# File paths
csv_path = '/scratch/k10/ef7927/research_project/csv/all_stations/extreme_events.csv'
base_nc_path = '/g/data/rt52/era5/single-levels/reanalysis/cin/{}/{}'
output_dir = '/scratch/k10/ef7927/research_project/netcdf/stations/999/'
os.makedirs(output_dir, exist_ok=True)

# Read the CSV file
df = pd.read_csv(csv_path)

# Initialize a dictionary to store CIN profiles by location
cin_profiles = {}

# Process each row
for _, row in df.iterrows():
    date = pd.to_datetime(row['date'])
    lon, lat = row['longitude'], row['latitude']
    year, month = date.year, date.month
    days_in_month = pd.Period(f"{year}-{month:02d}").days_in_month

    nc_file = f"cin_era5_oper_sfc_{year}{month:02d}01-{year}{month:02d}{days_in_month:02d}.nc"
    nc_file_path = base_nc_path.format(year, nc_file)

    if not os.path.exists(nc_file_path):
        print(f"NetCDF file not found: {nc_file_path}")
        continue

    print(f"Processing: {nc_file} (Lat: {lat}, Lon: {lon})")
    
    # Open dataset and extract CIN for the specific location and time
    ds = xr.open_dataset(nc_file_path)
    cin_data = ds['cin'].sel(time=date, longitude=lon, latitude=lat, method='nearest')
    ds.close()

    # Store CIN data grouped by location
    cin_profiles.setdefault((lon, lat), []).append(cin_data.drop_vars(['longitude', 'latitude']))

# Save CIN profiles
for (lon, lat), cin_data_list in cin_profiles.items():
    cin_array = xr.concat(cin_data_list, dim='time')
    ds_out = xr.Dataset({'cin': (['time'], cin_array.data)}, coords={'time': cin_array.time, 'longitude': lon, 'latitude': lat})
    
    nc_output_file = f"cin_profile_lon{lon}_lat{lat}.nc"
    ds_out.to_netcdf(os.path.join(output_dir, nc_output_file))

print("All CIN profiles saved.")

# === Merge NetCDF files into CSV ===
input_pattern = os.path.join(output_dir, "*cin*.nc")
output_csv = '/scratch/k10/ef7927/research_project/codes/cin/cin_extreme_days.csv'
os.makedirs(os.path.dirname(output_csv), exist_ok=True)

# Load multiple NetCDF files at once and convert them to DataFrame
ds_all = xr.open_mfdataset(input_pattern, combine='by_coords')
df_all = ds_all.to_dataframe().reset_index()[['time', 'longitude', 'latitude', 'cin']]
df_all.rename(columns={'time': 'date'}, inplace=True)
df_all.to_csv(output_csv, index=False)

print(f"Data successfully saved to {output_csv}")