import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import argparse
import os

VARIABLE_LABELS = {
    't2m':   ('2m Temperature',          '°C',  lambda x: x - 273.15),
    'mx2t':  ('2m Max Temperature',      '°C',  lambda x: x - 273.15),
    'mn2t':  ('2m Min Temperature',      '°C',  lambda x: x - 273.15),
    'd2m':   ('2m Dewpoint Temperature', '°C',  lambda x: x - 273.15),
    'tp':    ('Total Precipitation',     'mm',  lambda x: x * 1000),
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', required=True, help='path to .nc file')
    parser.add_argument('--out',  default='outputs/nc_preview.png')
    args = parser.parse_args()

    ds = xr.open_dataset(args.file)

    print("\n=== Dataset info ===")
    print(ds)
    print("\n=== Variables ===")
    for v in ds.data_vars:
        print(f"  {v}: {ds[v].dims}  shape={ds[v].shape}")

    # Squeeze time dim if present (monthly file = single timestep)
    vars_present = [v for v in VARIABLE_LABELS if v in ds]
    n = len(vars_present)

    fig, axes = plt.subplots(
        1, n, figsize=(5 * n, 4),
        subplot_kw={'projection': None}  # swap for cartopy if installed
    )
    if n == 1:
        axes = [axes]

    for ax, var in zip(axes, vars_present):
        label, unit, transform = VARIABLE_LABELS[var]
        data = ds[var].squeeze()   # drop size-1 time dim
        data_converted = transform(data.values)

        im = ax.pcolormesh(
            ds['longitude'], ds['latitude'], data_converted,
            cmap='RdYlBu_r' if 'temp' in label.lower() else 'Blues',
            shading='auto'
        )
        plt.colorbar(im, ax=ax, label=unit, shrink=0.8)
        ax.set_title(label, fontsize=10)
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')

        # Print stats
        print(f"\n{label}: min={data_converted.min():.2f} max={data_converted.max():.2f} {unit}")

    # Pull date from filename for title
    fname = os.path.basename(args.file)
    fig.suptitle(f'ERA5 BSW Ontario — {fname}', fontsize=12, y=1.02)
    plt.tight_layout()

    os.makedirs('outputs', exist_ok=True)
    plt.savefig(args.out, dpi=150, bbox_inches='tight')
    print(f"\nSaved → {args.out}")
    plt.show()

if __name__ == '__main__':
    main()