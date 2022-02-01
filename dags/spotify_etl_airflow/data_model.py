import pandas as pd 
import requests
from datetime import datetime, date
import datetime as dt
from spotify_etl_airflow.posgres_conn import get_engine_from_settings
from pangres import upsert

def create_date():
    yesterday = date.today() - dt.timedelta(days=1)
    yesterday = datetime.combine(yesterday, datetime.min.time())
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    return yesterday_unix_timestamp

class DataModel:
    def __init__(self, token):
        self.headers = {
            "Accept" : "application/json",
            "Content-Type" : "application/json",
            "Authorization" : "Bearer {token}".format(token=token)
            }
        self.date_timestamp = create_date()
        self.limit = 50

    def ingest_data(self):      
        r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit={limit}&after={time}".format(time=self.date_timestamp, limit=self.limit), headers = self.headers)
        
        return r.json()

    def extract_item(self, item, item_id, attbs):    
        r = requests.get("https://api.spotify.com/v1/{item}/{id}".format(id=item_id, item=item), headers=self.headers)
        data_item = r.json()

        return [data_item[attb] for attb in attbs]

    def extract_data(self, data, columns):
        df = pd.DataFrame(columns=["id","date","track_id","artist_id","track_name","popularity","duration_ms","artist_name","genres"])
        for item in data["items"]:
            id = item["played_at"]
            date = item["played_at"][0:10]
            track_id = item["track"]["id"]
            artist_id = item["track"]["artists"][0]["id"]

            track_name, popularity, duration_ms = self.extract_item("tracks", track_id, ["name","popularity","duration_ms"])
            artist_name, genres = self.extract_item("artists", artist_id, ["name","genres"])
            
            df.loc[len(df)] = [id, date, track_id, artist_id, track_name, popularity, duration_ms, artist_name, genres]
        
        df = df[columns]
        return df
    
    def load_data(self, df, table_name, dtype):
        engine = get_engine_from_settings()
        upsert(con=engine,
                df=df,
                table_name=table_name,
                if_row_exists='update',
                dtype=dtype,
                create_table=False)