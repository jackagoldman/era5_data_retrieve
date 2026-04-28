import cdsapi
import yaml
import argparse
import os
import zipfile
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed


def is_valid_nc(fpath, min_bytes=50_000):
    return os.path.exists(fpath) and os.path.getsize(fpath) > min_bytes


def download_by_month(c, cfg, month):
    out_dir = cfg['output_dir']
    os.makedirs(out_dir, exist_ok=True)

    # Final output files — one per stepType
    fname_avg = os.path.join(out_dir, f'era5_all_years_{month:02d}_avgua.nc')
    fname_minmax = os.path.join(out_dir, f'era5_all_years_{month:02d}_avgad.nc')

    if is_valid_nc(fname_avg) and is_valid_nc(fname_minmax):
        print(f'  skip month {month:02d} (exists and valid)')
        return

    zip_path = os.path.join(out_dir, f'era5_all_years_{month:02d}.zip')

    y_start, y_end = cfg['years']
    print(f'  downloading all years for month {month:02d}...')
    b = cfg['bbox']
    c.retrieve(
        'reanalysis-era5-single-levels-monthly-means',
        {
            'product_type': ['monthly_averaged_reanalysis'],
            'variable': [
                '2m_temperature',
                '2m_dewpoint_temperature',
                'maximum_2m_temperature_since_previous_post_processing',
                'minimum_2m_temperature_since_previous_post_processing',
                'total_precipitation',
            ],
            'year':  [str(y) for y in range(y_start, y_end + 1)],
            'month': [f'{month:02d}'],
            'time':  ['00:00'],
            'area':  [b['north'], b['west'], b['south'], b['east']],
            'data_format':     'netcdf',
            'download_format': 'zip',   # accept zip, we handle it
        },
        zip_path
    )

    # Unzip and rename by stepType
    with zipfile.ZipFile(zip_path, 'r') as z:
        for name in z.namelist():
            dest = fname_minmax if 'avgad' in name else fname_avg
            with z.open(name) as src, open(dest, 'wb') as dst:
                shutil.copyfileobj(src, dst)
            print(f'    extracted → {os.path.basename(dest)}')

    os.remove(zip_path)
    print(f'  done month {month:02d}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config',  required=True)
    parser.add_argument('--workers', type=int, default=5)
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    c = cdsapi.Client()

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(download_by_month, c, cfg, m): m for m in range(1, 13)}
        for fut in as_completed(futures):
            m = futures[fut]
            if fut.exception():
                print(f'  ERROR month {m:02d}: {fut.exception()}')


if __name__ == '__main__':
    main()