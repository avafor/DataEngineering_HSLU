"""Download one TLC monthly file into the shared data directory."""
import argparse
from pathlib import Path
from urllib.request import urlretrieve

parser = argparse.ArgumentParser()
parser.add_argument('--year', type=int, required=True)
parser.add_argument('--month', type=int, choices=range(1, 13), required=True)
args = parser.parse_args()
filename = f'yellow_tripdata_{args.year}-{args.month:02d}.parquet'
target = Path('/app/data') / 'source.parquet'
url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}'
print(f'Downloading {url}')
urlretrieve(url, target)
print(f'Extracted {target}')
