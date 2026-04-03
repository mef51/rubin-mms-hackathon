""" Cone Search using the Fink Broker to crossmatch to Rubin Alerts
@author: Yuxin (Vic) Dong
last edited: April 2, 2026 """
import requests
import io

import pandas as pd
import numpy as np
import seaborn as sns
import requests
import time
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
sns.set_context('talk')

APIURL = 'https://api.lsst.fink-portal.org'

# Cone search for a list of positions
def read_transient(html):
    df = pd.read_csv(html)
    name = np.array(df['tns_name']) 
    ra = np.array(df['ra']) 
    dec = np.array(df['dec'])
    return name, ra, dec

def rubin_conesearch(obj_name, ra, dec, radius: float):
    """
    Perform cone searches for multiple targets on Rubin LSST and return unique object links.
    
    Parameters
    ----------
    obj_name : array-like
        Names of the FRBs
    ra : array-like
        RA in deg
    dec : array-like
        Declinations in deg
    radius : float
        Search radius in arcsec

    Returns
    -------
    dict
        Dictionary keyed by object name, each containing a list of Rubin match links.
    """
    store_objs = {}

    for i in range(len(ra)):
        payload = {
            "ra": str(ra[i]),
            "dec": str(dec[i]),
            "columns": "r:diaObjectId", #,r:reliability" this is an unique object ID that could have multiple source id's (each alert) 
            "radius": radius,
        }
        try:
            r = requests.post(f"{APIURL}/api/v1/conesearch", json=payload)
            r.raise_for_status()
            pdf = pd.read_json(io.BytesIO(r.content))
            
            # Extract unique Rubin object IDs
            obj_ids = np.unique(pdf.get('r:diaObjectId', []))
            links = [f'https://lsst.fink-portal.org/{obj}' for obj in obj_ids]
        except Exception as e:
            print(f"Warning: failed for {obj_name[i]} ({ra[i]}, {dec[i]}): {e}")
            links = []

        store_objs[obj_name[i]] = {'Fink Links': links}
    
    #print(store_objs)
    return store_objs


def slack_message(match_results, radius):
    token = "your_token"
    channel = "your_channel"
    client = WebClient(token=token)
    timestamp = int(time.time())

    matches = []
    # collect only targets that have a result
    for frb, data in match_results.items():
        links = data.get("Fink Links", [])
        if len(links) > 0:
            matches.append((frb, links))

    # Case 1: no matches at all
    if len(matches) == 0:
        text = (
            f":duck: No positional coincidences found with Rubin transients "
            f"within {radius} arcsec on "
            f"<!date^{timestamp}^{{date_long}} at {{time}}|{time.ctime()}>."
        )
        try:
            client.chat_postMessage(channel=channel, text=text)
        except SlackApiError as e:
            print(f"Error posting Slack message: {e.response['error']}")
        return

    # Case 2: at least one match
    text = (
        f":volcano: Rubin transient coincidence(s) found within {radius} arcsec "
        f"on <!date^{timestamp}^{{date_long}} at {{time}}|{time.ctime()}>:\n\n"
    )

    for frb, links in matches:
        text += f"*{frb}*\n"
        for link in links:
            text += f"• {link}\n"
        text += "\n"

    try:
        client.chat_postMessage(channel=channel, text=text)
    except SlackApiError as e:
        print(f"Error posting Slack message: {e.response['error']}")

        
if __name__ == "__main__":
    radius = 10 ## default radius (in arcsec)
    html = 'your_data' #use browser link and change "edit?" to "export?format=csv&"
    frb_name, frb_ra, frb_dec = read_transient(html)
    results = rubin_conesearch(frb_name, frb_ra, frb_dec, radius=radius)
    text = slack_message(results, radius)
