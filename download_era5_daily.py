import cdsapi
import yaml
import argparse
import os
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed


def is_valid_nc(fpath, min_bytes=50_000):
    return os.path.exists(fpath) and os.path.getsize(fpath) > min_bytes


def download_tmax(c, cfg, year, month):
    out_dir = os.path.join(cfg['output_dir'], 'daily_tmax')
    os.makedirs(out_dir, exist_ok=True)
    fname = os.path.join(out_dir, f'era5_tmax_{year}_{month:02d}.nc')
    if is_valid_nc(fname):
        print(f'  skip tmax {year}-{month:02d}')
        return
    n_days = calendar.monthrange(year, month)[1]
    days   = [f'{d:02d}' for d in range(1, n_days + 1)]
    b      = cfg['bbox']
    print(f'  downloading tmax {year}-{month:02d}...')
    c.retrieve(
        'derived-era5-single-levels-daily-statistics',
        {
            'product_type':    'reanalysis',
            'variable':        ['maximum_2m_temperature_since_previous_post_processing'],
            'daily_statistic': 'daily_maximum',
            'time_zone':       'utc+00:00',
            'year':            [str(year)],
            'month':           [f'{month:02d}'],
            'day':             days,
            'area':            [b['north'], b['west'], b['south'], b['east']],
            'data_format':     'netcdf',
        },
        fname
    )
    print(f'  done tmax {year}-{month:02d}')


def download_tmin(c, cfg, year, month):
    out_dir = os.path.join(cfg['output_dir'], 'daily_tmin')
    os.makedirs(out_dir, exist_ok=True)
    fname = os.path.join(out_dir, f'era5_tmin_{year}_{month:02d}.nc')
    if is_valid_nc(fname):
        print(f'  skip tmin {year}-{month:02d}')
        return
    n_days = calendar.monthrange(year, month)[1]
    days   = [f'{d:02d}' for d in range(1, n_days + 1)]
    b      = cfg['bbox']
    print(f'  downloading tmin {year}-{month:02d}...')
    c.retrieve(
        'derived-era5-single-levels-daily-statistics',
        {
            'product_type':    'reanalysis',
            'variable':        ['minimum_2m_temperature_since_previous_post_processing'],
            'daily_statistic': 'daily_minimum',
            'time_zone':       'utc+00:00',
            'year':            [str(year)],
            'month':           [f'{month:02d}'],
            'day':             days,
            'area':            [b['north'], b['west'], b['south'], b['east']],
            'data_format':     'netcdf',
        },
        fname
    )
    print(f'  done tmin {year}-{month:02d}')


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

    # Download tmax and tmin in parallel — 5 workers across both
    tasks = [(y, m, 'max') for y, m in combos] + [(y, m, 'min') for y, m in combos]

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(download_tmax if t == 'max' else download_tmin, c, cfg, y, m): (y, m, t)
            for y, m, t in tasks
        }
        for fut in as_completed(futures):
            y, m, t = futures[fut]
            if fut.exception():
                print(f'  ERROR {t} {y}-{m:02d}: {fut.exception()}')


if __name__ == '__main__':
    main()