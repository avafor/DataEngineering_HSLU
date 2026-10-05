"""Check whether one monthly TLC taxi file is available."""

import argparse
from urllib.request import Request, urlopen


def check_source(year, month):
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

    request = Request(url, method="HEAD")
    with urlopen(request, timeout=20) as response:
        print(f"File is available: {url}")
        print(f"HTTP status: {response.status}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, choices=range(1, 13), required=True)
    args = parser.parse_args()
    check_source(args.year, args.month)


if __name__ == "__main__":
    main()
