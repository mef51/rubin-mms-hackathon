import polars as pl
from astropy.io import fits
import matplotlib.pyplot as plt
import io
import lsdb

df = pl.read_parquet('data/ftransfer_lsst_2026-04-01_19851/*')
print(df.head(10))
print(df.columns)

catalogs = {
    'ztf' : {
        'url': "https://data.lsdb.io/hats/ztf_dr14/ztf_object",
        'columns': ["ra", "dec", "ps1_objid", "nobs_r", "mean_mag_r"]
    },
    'tmass' : {
        'url': 'https://data.lsdb.io/hats/two_mass',
        'columns': ["ra", "dec"],
    },
    'delve' : {
        'url': 'https://data.lsdb.io/hats/delve/delve_dr3_gold',
        'columns': ["ra", "dec"],
    },
    'des' : {
        'url': 's3://stpubdata/des/public/hats/des_y6_gold',
        'columns': ["ra", "dec"],
    },
    'desi' : {
        'url': 'https://data.lsdb.io/hats/desi/desi_dr1_zcat',
        'columns': ["ra", "dec"],
    },
    'erass' : { # x-ray
        'url': 'https://data.lsdb.io/hats/erosita', ,
        'columns': ["ra", "dec"],
    },
    'euclid' : {
        'url': 'https://data.lsdb.io/hats/euclid_q1',
        'columns': ["ra", "dec"],
    },
    'gaia' : {
        'url': 's3://stpubdata/gaia/gaia_dr3/public/hats',
        'columns': ["ra", "dec"],
    },
    'panstarrs' : {
        'url': 's3://stpubdata/panstarrs/ps1/public/hats/otmo',
        'columns': ["ra", "dec"],
    },
    'sdss' : {
        'url': 'https://data.lsdb.io/hats/sdss_dr7_spectra',
        'columns': ["ra", "dec"],
    },
    'skymapper' : {
        'url': 'https://data.lsdb.io/hats/skymapper_dr4/catalog',
        'columns': ["ra", "dec"],
    },
    'tess' : {
        'url': 's3://stpubdata/tess/public/hats/tic/',
        'columns': ["ra", "dec"],
    },
    'tns' : {
        'url': 'https://data.lsdb.io/hats/tns',
        'columns': ["ra", "dec"],
    },
    'vsx' : {
        'url': 'https://data.lsdb.io/hats/vsx',
        'columns': ["ra", "dec"],
    },
    'neowise' : {
        'url': 'https://data.lsdb.io/hats/wise/neowise',
        'columns': ["ra", "dec"],
    },
    'allwise' : {
        'url': 'https://data.lsdb.io/hats/wise/allwise',
        'columns': ["ra", "dec"],
    }
}

def crossmatch(ra, dec, radius_arcsec=3):
    crossmatch_dfs = {}
    for catalog in catalogs:
        # print(catalog, catalogs[catalog]['columns'])
        try:
            lazycat = lsdb.open_catalog(
                catalogs[catalog]['url'],
                # columns=catalogs[catalog]['columns'],
                search_filter=lsdb.ConeSearch(ra, dec, radius_arcsec)
            )
            # tdf = lazycat.head() # 5 rows
            tdf = lazycat.compute()
            # print(catalog)
            # print(tdf)
            crossmatch_dfs[catalog] = tdf
        except ValueError as e:
            print(e)
    return crossmatch_dfs

if __name__ == "__main__":
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
        crossmatch(ra, dec)
        break
