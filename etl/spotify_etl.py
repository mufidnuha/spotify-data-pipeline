from data_model import DataModel
from transformer import PlayedTracksTransformer, TracksArtistsTransformer
from sqlalchemy import VARCHAR, DATE, ARRAY, INTEGER

def spotify_etl(token):
    played_tracks_dict= {
        "dtype": {'id':VARCHAR(200),
                    'date':DATE,
                    'track_id':VARCHAR(200),
                    'artist_id':VARCHAR(200)
                },
        "columns": ["id","date","track_id","artist_id"],
        "table_name": "played_tracks"
    }

    tracks_dict = {
        "dtype": {'id':VARCHAR(200), 
                    'name':VARCHAR(200), 
                    'popularity':INTEGER, 
                    'duration_ms':INTEGER},
        "columns": ["track_id","date","track_name","popularity","duration_ms"],
        "table_name": "tracks"
    }

    artists_dict = {
        "dtype": {'id':VARCHAR(200), 
                    'name':VARCHAR(200), 
                    'genres':ARRAY(VARCHAR)},
        "columns": ["artist_id","date","artist_name","genres"],
        "table_name": "artists"
    }

    data_model = DataModel(token)
    pt_transformer = PlayedTracksTransformer()
    ta_transformer = TracksArtistsTransformer()

    played_tracks_df = data_model.extract_data(data_model.ingest_data(), played_tracks_dict["columns"])
    tracks_df = data_model.extract_data(data_model.ingest_data(), tracks_dict["columns"])
    artists_df = data_model.extract_data(data_model.ingest_data(), artists_dict["columns"])

    played_tracks_df = pt_transformer.prepare_data(played_tracks_df)
    tracks_df = ta_transformer.prepare_data(tracks_df, ["track_id","track_name"])
    artists_df = ta_transformer.prepare_data(artists_df, ["artist_id","artist_name"])

    data_model.load_data(tracks_df, tracks_dict["table_name"], tracks_dict["dtype"])
    data_model.load_data(artists_df, artists_dict["table_name"], artists_dict["dtype"])
    data_model.load_data(played_tracks_df, played_tracks_dict["table_name"], played_tracks_dict["dtype"])

if __name__ == "__main__":
    spotify_etl()

