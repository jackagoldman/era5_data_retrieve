import xarray as xr
import geopandas as gpd
import pandas as pd
import numpy as np
import yaml
import argparse
import os

def extract(cfg, fires_path):
    fires = gpd.read_file(fires_path).to_crs('EPSG:4326')
    fires['lon']   = fires.geometry.centroid.x
    fires['lat']   = fires.geometry.centroid.y
    fires['year']  = pd.to_datetime(fires['ignition_date']).dt.year
    fires['month'] = pd.to_datetime(fires['ignition_date']).dt.month

    out_dir = cfg['output_dir']
    results = []

    for (year, month), group in fires.groupby(['year', 'month']):
        fpath = os.path.join(out_dir, f'era5_{year}_{month:02d}.nc')
        if not os.path.exists(fpath):
            print(f'  WARNING: missing {fpath}, skipping')
            continue

        ds = xr.open_dataset(fpath)
        lats = xr.DataArray(group['lat'].values, dims='fire')
        lons = xr.DataArray(group['lon'].values, dims='fire')
        ex   = ds.sel(latitude=lats, longitude=lons, method='nearest')

        T_dew  = ex['d2m'].values  - 273.15
        T_mean = ex['t2m'].values  - 273.15
        ea = 0.6108 * np.exp(17.27 * T_dew  / (T_dew  + 237.3))
        es = 0.6108 * np.exp(17.27 * T_mean / (T_mean + 237.3))

        df = pd.DataFrame({
            'fire_id':   group.index,
            'year':      year,
            'month':     month,
            'T_mean_C':  T_mean,
            'T_max_C':   ex['mx2t'].values - 273.15,
            'T_min_C':   ex['mn2t'].values - 273.15,
            'T_dew_C':   T_dew,
            'precip_mm': ex['tp'].values * 1000,
            'VPD_kPa':   es - ea,
        })
        results.append(df)
        ds.close()

    out = pd.concat(results).reset_index(drop=True)
    out_path = os.path.join('outputs', f"{cfg['name']}_cmi_vars.csv")
    os.makedirs('outputs', exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f'Saved {len(out)} records → {out_path}')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config',  required=True)
    parser.add_argument('--fires',   required=True, help='path to fire perimeters gpkg/shp')
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    extract(cfg, args.fires)

if __name__ == '__main__':
    main()