import polars as pl

df = pl.read_parquet('data/ftransfer_lsst_2026-04-01_19851/*')
print(df.head(10))
print(df.columns)

for row in df.iter_rows(named=True):
    ra, dec = row['diaSource']['ra'], row['diaSource']['dec']
    cutouts = [
        row['cutoutDifference'],
        row['cutoutScience'],
        row['cutoutTemplate']
    ]
    for i, cutout in enumerate(cutouts):
        with fits.open(io.BytesIO(cutout)) as fields:
            img      = fields[0].data
            _img_unc = fields[1].data
            _img_psf = fields[2].data
            plt.imshow(img)
            plt.show()
            plt.close()

    print(ra, dec)
    break
