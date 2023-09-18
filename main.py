import os
import geopandas as gpd
from sqlalchemy import create_engine

db_params = {
    "dbname": "test",
    "user": "postgres",
    "password": "admin",
    "host": "localhost",
    "port": "5432",
}

parent_directory = "us"
table_name = "us"


for state_folder in os.listdir(parent_directory):
    state_folder_path = os.path.join(parent_directory, state_folder)

    geojson_file_name = f"statewide-addresses-state.geojson"

    geojson_file_path = os.path.join(state_folder_path, geojson_file_name)

    if os.path.exists(geojson_file_path):
        gdf = gpd.read_file(geojson_file_path, rows=10, engine="pyogrio")

        state_abbreviation = state_folder.upper()
        gdf["state"] = state_abbreviation

        engine = create_engine(
            f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        )
        gdf.to_postgis(table_name, engine, if_exists='append', index=False)

        engine.dispose()

        print(f"Processed file: {table_name} (state: {state_abbreviation}) in folder {state_folder}")
    else:
        print(f"File not found: {geojson_file_name} in folder {state_folder}")

print("All files processed.")
