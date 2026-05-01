import cdsapi
import yaml
import argparse
import os
import calendar
import zipfile
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed


def is_valid_nc(fpath, min_bytes=50_000):
    return os.path.exists(fpath) and os.path.getsize(fpath) > min_bytes


def download_minmax(c, cfg, year, month):
    """Download daily T_max and T_min for one year-month"""
    out_dir = os.path.join(cfg['output_dir'], 'daily_min')# switch to daily_min or daily max
    os.makedirs(out_dir, exist_ok=True)

    fname = os.path.join(out_dir, f'era5_min_{year}_{month:02d}.nc') # set to min or max based on temp variable

    if is_valid_nc(fname):
        print(f'  skip {year}-{month:02d} (exists)')
        return

    n_days = calendar.monthrange(year, month)[1]
    days   = [f'{d:02d}' for d in range(1, n_days + 1)]
    b      = cfg['bbox']
    tmp_path = os.path.join(out_dir, f'era5_min_{year}_{month:02d}.tmp') # specify min or max in the name

    print(f'  downloading T_max/T_min {year}-{month:02d} ({n_days} days)...')
    c.retrieve(
        'derived-era5-single-levels-daily-statistics',
        {
            'product_type':    'reanalysis',
            'daily_statistic': 'daily_mean',   # required field
            'time_zone':       'utc+00:00',    # required field
            'variable': [
               # 'maximum_2m_temperature_since_previous_post_processing',   # select min or max by commenting out
                'minimum_2m_temperature_since_previous_post_processing',
            ],
            'year':   [str(year)],
            'month':  [f'{month:02d}'],
            'day':    days,
            'area':   [b['north'], b['west'], b['south'], b['east']],
        },
        tmp_path
    )

    # Handle whatever format CDS returns
    with open(tmp_path, 'rb') as f:
        header = f.read(4)

    if header[:4] == b'PK\x03\x04':
        print(f'  got zip, extracting...')
        with zipfile.ZipFile(tmp_path, 'r') as z:
            nc_files = [n for n in z.namelist() if n.endswith('.nc')]
            if nc_files:
                with z.open(nc_files[0]) as src, open(fname, 'wb') as dst:
                    shutil.copyfileobj(src, dst)
            else:
                print(f'  WARNING: no .nc in zip, contents: {z.namelist()}')
        os.remove(tmp_path)
    elif header[:4] == b'\x89HDF':
        os.rename(tmp_path, fname)
    else:
        print(f'  WARNING: unknown format header={header}')

    print(f'  done {year}-{month:02d}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config',  required=True)
    parser.add_argument('--workers', type=int, default=5)
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    c = cdsapi.Client()
    y_start, y_end = cfg['years']

    combos = [(y, m) for y in range(y_start, y_end + 1) for m in range(1, 13)]

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(download_minmax, c, cfg, y, m): (y, m)
            for y, m in combos
        }
        for fut in as_completed(futures):
            y, m = futures[fut]
            if fut.exception():
                print(f'  ERROR {y}-{m:02d}: {fut.exception()}')


if __name__ == '__main__':
    main()