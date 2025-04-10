import os
import pandas as pd
import requests
import argparse

def download_file(url, save_path):
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
        print(f"Downloaded: {save_path}")
    else:
        print(f"Failed to download: {url}")

def main(time_resolution="day", data_type="sites", file_format="csv"):
    if data_type == "sites":
        metadata_url = "https://thredds.geus.dk/thredds/fileServer/aws/metadata/AWS_sites_metadata.csv"
        metadata_path = "metadata/AWS_sites_metadata.csv"
        base_url_csv = "https://thredds.geus.dk/thredds/fileServer/aws/l3sites/csv/"
        base_url_nc = "https://thredds.geus.dk/thredds/fileServer/aws/l3sites/netcdf/"
        save_dir_csv = f"level_3_sites/csv/{time_resolution}/"
        save_dir_nc = f"level_3_sites/netcdf/{time_resolution}/"
        id_column = "site_id"
    elif data_type == "stations":
        metadata_url = "https://thredds.geus.dk/thredds/fileServer/aws/metadata/AWS_stations_metadata.csv"
        metadata_path = "metadata/AWS_stations_metadata.csv"
        base_url_csv = "https://thredds.geus.dk/thredds/fileServer/aws/l2stations/csv/"
        save_dir_csv = f"level_2_stations/csv/{time_resolution}/"
        id_column = "station_id"
    else:
        print("Invalid data type. Choose 'sites' or 'stations'.")
        return

    # Download metadata file
    download_file(metadata_url, metadata_path)

    # Read IDs from metadata
    df = pd.read_csv(metadata_path)
    ids = df[id_column].dropna().unique()

    # Download files for each ID
    for id_value in ids:
        if file_format in ["csv", "both"]:
            file_url_csv = f"{base_url_csv}{time_resolution}/{id_value}_{time_resolution}.csv"
            file_path_csv = os.path.join(save_dir_csv, f"{id_value}_{time_resolution}.csv")
            download_file(file_url_csv, file_path_csv)

        if data_type == "sites" and file_format in ["nc","netcdf", "both"]:
            file_url_nc = f"{base_url_nc}{time_resolution}/{id_value}_{time_resolution}.nc"
            file_path_nc = os.path.join(save_dir_nc, f"{id_value}_{time_resolution}.nc")
            download_file(file_url_nc, file_path_nc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the script with optional parameters.")
    parser.add_argument("--time_resolution", type=str, default="day", help="Time resolution (default: day)")
    parser.add_argument("--data_type", type=str, default="sites", help="Data type (default: sites)")
    parser.add_argument("--file_format", type=str, default="csv", help="File format (default: csv)")

    args = parser.parse_args()

    main(time_resolution=args.time_resolution, data_type=args.data_type, file_format=args.file_format)
