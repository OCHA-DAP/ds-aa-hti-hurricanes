import os

from src.datasources.chirps_gefs import (
    download_recent_chirps_gefs,
    process_recent_chirps_gefs,
)

if __name__ == "__main__":
    print(os.getenv("TEST1"))
    print(os.getenv("TEST2"))
    print(os.getenv("TEST3"))
    download_recent_chirps_gefs()
    process_recent_chirps_gefs()
