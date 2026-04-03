import polars as pl
from astropy.io import fits
import matplotlib.pyplot as plt
import io
import lsdb
import numpy as np
import pyvo as vo
import pandas as pd
#from astropy.coordinates import SkyCoord
import astropy.units as u
import urllib.request
import urllib.parse
from astroquery.simbad import Simbad
from astropy import units as u
from astropy.coordinates import SkyCoord

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


def check_nrao(ra_query, dec_query):

    # IRSA
    irsa_service = vo.dal.TAPService("https://irsa.ipac.caltech.edu/TAP")
    search_radius = 5.0 / 3600.0  # arcsec → deg

    irsa_result = irsa_service.search("""
        SELECT ra,dec,j_m,j_msigcom,h_m,h_msigcom,k_m,k_msigcom,ph_qual,cc_flg
        FROM fp_psc
        WHERE CONTAINS(
            POINT('ICRS',ra,dec),
            CIRCLE('ICRS',""" + ra_query + "," + dec_query + "," + str(search_radius) + """)
        )=1
    """)
    tab = irsa_result.to_table()
    print("IRSA: None" if len(tab) == 0 else f"IRSA: Found {len(tab)} result(s)")
    # NRAO archive
    nrao_service = vo.dal.TAPService("https://data-query.nrao.edu/tap")
    query="SELECT * FROM tap_schema.obscore WHERE CONTAINS(POINT('ICRS',s_ra,s_dec),CIRCLE('ICRS',"+ra_query+","+dec_query+","+str(search_radius)+"))=1" 

    # NRAO archive
    nrao_service = vo.dal.TAPService("https://data-query.nrao.edu/tap")
    
    query = f"""
    SELECT * FROM tap_schema.obscore
    WHERE CONTAINS(
        POINT('ICRS',s_ra,s_dec),
        CIRCLE('ICRS',{ra_query},{dec_query},{search_radius})
    )=1
    """
    
    nrao_result = nrao_service.search(query)
    output = nrao_result.to_table()
    
    if len(output) == 0:
        print("NRAO: None")
        return
    
    instruments = output['instrument_name'].astype(str)
    
    # Check for VLA and ALMA
    has_vla = np.any(np.char.find(instruments, 'VLA') >= 0)
    has_alma = np.any(np.char.find(instruments, 'ALMA') >= 0)
    
    print("VLA: Found" if has_vla else "VLA: None")
    print("ALMA: Found" if has_alma else "ALMA: None")
    
    # Continue printing the EB summary
    unique_EBs = np.unique(output['obs_publisher_did'])
    print(f"Unique EBs: {len(unique_EBs)}")
    
    for i, EB in enumerate(unique_EBs):
        EB_index = np.where(output['obs_publisher_did'] == EB)
    
        total_tos_per_EB = np.sum(output['t_exptime'][EB_index[0]])
        freq_min = output['freq_min'][EB_index[0][0]]
        freq_max = output['freq_max'][EB_index[0][0]]
        instrument = output['instrument_name'][EB_index[0][0]]
    
        print('Instrument: {}, EB: {}, TOS: {:0.1f}s, Nu_min - Nu_max: {:0.2f}-{:0.2f} GHz' \
         .format(instrument,unique_EBs[i],total_tos_per_EB,freq_min/1.0e9,freq_max/1.0e9)
    )

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
