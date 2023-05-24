# immudb web API tests

## Requirements

1. [Python](https://www.python.org) 3.10
2. [Poetry](https://python-poetry.org/) 1.4.2

## Quick start

1. Install everything with `poetry install`.
2. Run `poetry run python main.py -h` to see all arguments available for configuration:

```shell
usage: main.py [-h] [-d DURATION] [-w WORKERS] [-u USERNAME] [-p PASSWORD] [-db DATABASE] -b BASE_URL [-s DOCUMENTS_SIZE]

Load test tool for immudb web API

options:
  -h, --help            show this help message and exit
  -d DURATION, --duration DURATION
                        Duration of tests execution (default: 10)
  -w WORKERS, --workers WORKERS
                        Number of concurrent workers (default: 1)
  -u USERNAME, --username USERNAME
                        immudb username (default: immudb)
  -p PASSWORD, --password PASSWORD
                        immudb password (default: immudb)
  -db DATABASE, --database DATABASE
                        immudb database (default: defaultdb)
  -b BASE_URL, --base_url BASE_URL
                        Host url for web API (default: None)
  -s DOCUMENTS_SIZE, --documents_size DOCUMENTS_SIZE
                        Number of documents to send in a single request (default: 1)
```

**Basic example**: `poetry run python main.py -b http://localhost:8091`