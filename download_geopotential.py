import cdsapi
c = cdsapi.Client()





c.retrieve(
    'reanalysis-era5-single-levels-monthly-means',
    {
        'product_type': ['monthly_averaged_reanalysis'],
        'variable':     ['geopotential'],
        'year':         ['1990'],
        'month':        ['01'],
        'time':         ['00:00'],
        'area':         [54.88689352, -95.15367601, 48.0566673, -85.0234456],
        'data_format':     'netcdf',
        'download_format': 'unarchived',
    },
    'data/era5/bsw_ontario/era5_geopotential.nc'
)


# Check the metadata variable list
variables = ["maximum_2m_temperature_since_previous_post_processing",
             "minimum_2m_temperature_since_previous_post_processing"]

# Test download from monthly means
import cdsapi
c = cdsapi.Client()
c.retrieve(
    'reanalysis-era5-single-levels-monthly-means',
    {
        'product_type': ['monthly_averaged_reanalysis'],
        'variable': [
            'maximum_2m_temperature_since_previous_post_processing',
            'minimum_2m_temperature_since_previous_post_processing',
        ],
        'year':  ['1990'],
        'month': ['07'],
        'time':  ['00:00'],
        'area':  [54.88689352, -95.15367601, 48.0566673, -85.0234456],
        'data_format':     'netcdf',
        'download_format': 'zip',
    },
    'test_tmax_tmin_monthly.zip'
)


c = cdsapi.Client()

# Test mx2t with daily_maximum
c.retrieve(
    'derived-era5-single-levels-daily-statistics',
    {
        'product_type':    'reanalysis',
        'variable':        ['maximum_2m_temperature_since_previous_post_processing'],
        'daily_statistic': 'daily_maximum',
        'time_zone':       'utc+00:00',
        'year':            ['1990'],
        'month':           ['07'],
        'day':             [f'{d:02d}' for d in range(1, 32)],
        'area':            [54.88689352, -95.15367601, 48.0566673, -85.0234456],
        'data_format':     'netcdf',
    },
    'test_mx2t_daily.nc'
)


import xarray as xr
ds = xr.open_dataset('test_mx2t_daily.nc', engine='h5netcdf')
print(ds)
print(list(ds.data_vars))
mx2t = ds[list(ds.data_vars)[0]] - 273.15
print(f'mx2t mean: {float(mx2t.mean()):.2f} °C')
print(f'mx2t min:  {float(mx2t.min()):.2f} °C')
print(f'mx2t max:  {float(mx2t.max()):.2f} °C')




c.retrieve(
    'derived-era5-single-levels-daily-statistics',
    {
        'product_type':    'reanalysis',
        'variable':        ['minimum_2m_temperature_since_previous_post_processing'],
        'daily_statistic': 'daily_minimum',
        'time_zone':       'utc+00:00',
        'year':            ['1990'],
        'month':           ['07'],
        'day':             [f'{d:02d}' for d in range(1, 32)],
        'area':            [54.88689352, -95.15367601, 48.0566673, -85.0234456],
        'data_format':     'netcdf',
    },
    'test_mn2t_daily.nc'
)

ds = xr.open_dataset('test_mn2t_daily.nc', engine='h5netcdf')
mn2t = ds[list(ds.data_vars)[0]] - 273.15
print(f'mn2t mean: {float(mn2t.mean()):.2f} °C')
print(f'mn2t min:  {float(mn2t.min()):.2f} °C')
print(f'mn2t max:  {float(mn2t.max()):.2f} °C')
