import os
import pandas as pd
import xarray as xr
import numpy as np
from calendar import monthrange

# Define paths
station_file = '/scratch/k10/ef7927/research_project/csv/all_stations/stations_coordinate.csv'
temp_data_path = '/g/data/rt52/era5/pressure-levels/reanalysis/t/'
rh_data_path = '/g/data/rt52/era5/pressure-levels/reanalysis/r/'
output_file = '/scratch/k10/ef7927/research_project/codes/kindex/kindex_all_days.nc'

# Load station data
stations_df = pd.read_csv(station_file).rename(columns=lambda x: x.strip())

# Validate required columns
if not {'longitude', 'latitude'}.issubset(stations_df.columns):
    raise ValueError("Missing required columns: 'longitude' and 'latitude'")

# Function to construct NetCDF file path
def get_netcdf_file(base_path, var, year, month):
    days = monthrange(year, month)[1]
    return os.path.join(base_path, str(year), f"{var}_era5_oper_pl_{year}{month:02d}01-{year}{month:02d}{days}.nc")

# Function to compute dew point temperature
def dew_point(T, RH):
    return T - ((100 - RH) / 5.0)

# Pressure levels for K-Index calculation
pressure_levels = [500, 700, 850]

# Initialize a list to collect all station data
all_k_index_data = []

# Process each station
for _, station in stations_df.iterrows():
    lon, lat = station['longitude'], station['latitude']

    for year in range(2015, 2024):
        for month in range(1, 13):
            temp_file = get_netcdf_file(temp_data_path, 't', year, month)
            rh_file = get_netcdf_file(rh_data_path, 'r', year, month)

            if not os.path.exists(temp_file) or not os.path.exists(rh_file):
                print(f"Missing data for {year}-{month:02d}, skipping.")
                continue

            with xr.open_dataset(temp_file) as temp_data, xr.open_dataset(rh_file) as rh_data:
                for day in range(1, monthrange(year, month)[1] + 1):
                    for hour in range(24):
                        time = f"{year}-{month:02d}-{day:02d}T{hour:02d}:00"
                        temp = temp_data.sel(time=time, longitude=lon, latitude=lat, level=pressure_levels, method='nearest')
                        rh = rh_data.sel(time=time, longitude=lon, latitude=lat, level=[700, 850], method='nearest')

                        T_500, T_700, T_850 = temp.t.sel(level=500).values - 273.15, \
                                              temp.t.sel(level=700).values - 273.15, \
                                              temp.t.sel(level=850).values - 273.15
                        TD_700 = dew_point(T_700, rh.r.sel(level=700).values)
                        TD_850 = dew_point(T_850, rh.r.sel(level=850).values)

                        K_index = (T_850 - T_500) + TD_850 - (T_700 - TD_700)

                        all_k_index_data.append({'time': time, 'longitude': lon, 'latitude': lat, 'K_index': K_index})

# Convert all collected data into a DataFrame
if all_k_index_data:
    df = pd.DataFrame(all_k_index_data)
    df['time'] = pd.to_datetime(df['time'])

    # Convert DataFrame to an xarray Dataset
    k_index_ds = xr.Dataset(
        {'K_index': (['time', 'longitude', 'latitude'], df.pivot_table(index='time', columns=['longitude', 'latitude'], values='K_index').values)},
        coords={'time': df['time'].values, 'longitude': df['longitude'].unique(), 'latitude': df['latitude'].unique()}
    )

    # Save the combined dataset to a single NetCDF file
    k_index_ds.to_netcdf(output_file)
    print(f"All K-Index data saved to {output_file}")