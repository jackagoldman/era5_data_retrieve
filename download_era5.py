import cdsapi
import yaml
import argparse
import os
from itertools import product
from concurrent.futures import ThreadPoolExecutor, as_completed

def download_month(c, cfg, year, month):
    out_dir = cfg['output_dir']
    os.makedirs(out_dir, exist_ok=True)
    fname = os.path.join(out_dir, f'era5_{year}_{month:02d}.nc')
    if os.path.exists(fname):
        print(f'  skip {year}-{month:02d} (exists)')
        return fname
    print(f'  downloading {year}-{month:02d}...')
    b = cfg['bbox']
    c.retrieve(
        'reanalysis-era5-single-levels-monthly-means',
        {
            'product_type': 'monthly_averaged_reanalysis',
            'variable': [
                '2m_temperature',
                '2m_dewpoint_temperature',
                'maximum_2m_temperature_since_previous_post_processing',
                'minimum_2m_temperature_since_previous_post_processing',
                'total_precipitation',
            ],
            'year': str(year),
            'month': f'{month:02d}',
            'time': '00:00',
            'area': [b['north'], b['west'], b['south'], b['east']],
            'format': 'netcdf',
        },
        fname
    )
    return fname

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--workers', type=int, default=5)
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    c = cdsapi.Client()
    y_start, y_end = cfg['years']
    combos = list(product(range(y_start, y_end + 1), range(1, 13)))

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(download_month, c, cfg, y, m): (y, m) for y, m in combos}
        for fut in as_completed(futures):
            y, m = futures[fut]
            if fut.exception():
                print(f'  ERROR {y}-{m}: {fut.exception()}')

if __name__ == '__main__':
    main()