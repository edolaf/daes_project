import pandas as pd
import xarray as xr
import os
import glob

# File paths
csv_path = '/scratch/k10/ef7927/research_project/csv/all_stations/extreme_events.csv'
base_nc_path = '/g/data/rt52/era5/single-levels/reanalysis/cape/{}/{}'
output_dir = '/scratch/k10/ef7927/research_project/netcdf/stations/999/'
os.makedirs(output_dir, exist_ok=True)

# Read the CSV file
df = pd.read_csv(csv_path)

# Initialize a dictionary to store CAPE profiles by location
cape_profiles = {}

# Process each row
for _, row in df.iterrows():
    date = pd.to_datetime(row['date'])
    lon, lat = row['longitude'], row['latitude']
    year, month = date.year, date.month
    days_in_month = pd.Period(f"{year}-{month:02d}").days_in_month

    nc_file = f"cape_era5_oper_sfc_{year}{month:02d}01-{year}{month:02d}{days_in_month:02d}.nc"
    nc_file_path = base_nc_path.format(year, nc_file)

    if not os.path.exists(nc_file_path):
        print(f"NetCDF file not found: {nc_file_path}")
        continue

    print(f"Processing: {nc_file} (Lat: {lat}, Lon: {lon})")
    
    # Open dataset and extract CAPE for the specific location and time
    ds = xr.open_dataset(nc_file_path)
    cape_data = ds['cape'].sel(time=date, longitude=lon, latitude=lat, method='nearest')
    ds.close()

    # Store CAPE data grouped by location
    cape_profiles.setdefault((lon, lat), []).append(cape_data.drop_vars(['longitude', 'latitude']))

# Save CAPE profiles
for (lon, lat), cape_data_list in cape_profiles.items():
    cape_array = xr.concat(cape_data_list, dim='time')
    ds_out = xr.Dataset({'cape': (['time'], cape_array.data)}, coords={'time': cape_array.time, 'longitude': lon, 'latitude': lat})
    
    nc_output_file = f"cape_profile_lon{lon}_lat{lat}.nc"
    ds_out.to_netcdf(os.path.join(output_dir, nc_output_file))

print("All CAPE profiles saved.")

# === Merge NetCDF files into CSV ===
input_pattern = os.path.join(output_dir, "*cape*.nc")
output_csv = '/scratch/k10/ef7927/research_project/codes/cape/cape_extreme_days.csv'
os.makedirs(os.path.dirname(output_csv), exist_ok=True)

# Load multiple NetCDF files at once and convert them to DataFrame
ds_all = xr.open_mfdataset(input_pattern, combine='by_coords')
df_all = ds_all.to_dataframe().reset_index()[['time', 'longitude', 'latitude', 'cape']]
df_all.rename(columns={'time': 'date'}, inplace=True)
df_all.to_csv(output_csv, index=False)

print(f"Data successfully saved to {output_csv}")