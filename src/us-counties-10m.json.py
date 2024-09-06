import geopandas as gpd
import sys
import json
from topojson import Topology
import requests
from pathlib import Path


URL = "https://raw.githubusercontent.com/observablehq/plot/09edfec1f26da02eeeb451f008dd2eb9fb8d8ffc/test/data/us-counties-10m.json"
TMPDIR = Path('src/.observablehq/cache/us-counties-10m.json')

r = requests.get(url=URL,  headers={'User-Agent': 'Mozilla/5.0'})


sys.stdout.write(json.dumps(r.json()))