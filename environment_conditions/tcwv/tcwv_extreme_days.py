import pandas as pd
import xarray as xr
import os
import glob

# File paths
csv_path = '/scratch/k10/ef7927/research_project/csv/all_stations/stations_coordinate.csv'
base_dir = '/g/data/rt52/era5/single-levels/reanalysis/tcwv/'
output_nc_dir = '/scratch/k10/ef7927/research_project/netcdf/stations/999/'
output_csv_dir = '/scratch/k10/ef7927/research_project/codes/tcwv/'

# Load station data
df = pd.read_csv(csv_path)

# Ensure output directory exists
os.makedirs(output_nc_dir, exist_ok=True)
os.makedirs(output_csv_dir, exist_ok=True)

# Extract TCWV profiles
tcwv_profiles = {}
for _, row in df.iterrows():
    date = pd.to_datetime(row['date'])
    lon, lat = row['longitude'], row['latitude']
    
    # Construct NetCDF file path
    days_in_month = pd.Period(f"{date.year}-{date.month:02d}").days_in_month
    nc_file = f"tcwv_era5_oper_sfc_{date.year}{date.month:02d}01-{date.year}{date.month:02d}{days_in_month:02d}.nc"
    nc_path = os.path.join(base_dir, str(date.year), nc_file)

    if not os.path.exists(nc_path):
        print(f"NetCDF file not found: {nc_path}")
        continue

    with xr.open_dataset(nc_path) as ds:
        if 'tcwv' not in ds:
            print(f"'tcwv' variable missing in {nc_path}")
            continue

        tcwv = ds['tcwv'].sel(time=date, longitude=lon, latitude=lat, method='nearest')
        tcwv_profiles.setdefault((lon, lat), []).append(tcwv.drop_vars(['longitude', 'latitude']))

# Save TCWV profiles to NetCDF
for (lon, lat), tcwv_list in tcwv_profiles.items():
    ds_out = xr.Dataset({'tcwv': (['time'], xr.concat(tcwv_list, dim='time').data)},
                        coords={'time': tcwv_list[0].time, 'longitude': lon, 'latitude': lat})
    
    nc_output_path = os.path.join(output_nc_dir, f'tcwv_profile_lon{lon}_lat{lat}.nc')
    ds_out.to_netcdf(nc_output_path)
    print(f"Saved TCWV profile to {nc_output_path}")

# Aggregate TCWV data and save to CSV
all_data = pd.concat([xr.open_dataset(nc).to_dataframe().reset_index()[['time', 'longitude', 'latitude', 'tcwv']]
                      .rename(columns={'time': 'date'}) for nc in glob.glob(f"{output_nc_dir}/*tcwv*.nc")],
                     ignore_index=True)

output_csv_path = os.path.join(output_csv_dir, 'tcwv_extreme_days.csv')
all_data.to_csv(output_csv_path, index=False)
print(f"Data successfully saved to {output_csv_path}")