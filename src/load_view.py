from posgres_conn import get_engine_from_settings

def load_view_top_five():
    engine = get_engine_from_settings()
    query_top_genres = """CREATE OR REPLACE VIEW top_genres AS
                            SELECT UNNEST(genres) as genres, COUNT(genres) AS genre_num
                            FROM played_tracks pt
                                JOIN artists a
                                ON pt.artist_id = a.id
                            WHERE pt."date" = (SELECT CURRENT_DATE - INTEGER '1' AS yesterday_date)
                            GROUP BY genres
                            ORDER BY genre_num DESC
                            LIMIT 5"""

    query_top_tracks = """CREATE OR REPLACE VIEW top_tracks AS
                            SELECT pt.track_id, t."name", COUNT(pt.track_id) AS track_num
                            FROM played_tracks pt 
                                JOIN tracks t 
                                ON pt.track_id = t.id 
                            WHERE pt."date" = (SELECT CURRENT_DATE - INTEGER '1' AS yesterday_date)
                            GROUP BY pt.track_id, t."name"
                            ORDER BY track_num DESC
                            LIMIT 5"""

    query_top_artists = """CREATE OR REPLACE VIEW top_artists AS
                            SELECT pt.artist_id, a."name", COUNT(pt.artist_id) AS artists_num 
                            FROM played_tracks pt 
                                JOIN artists a
                                ON pt.artist_id = a.id
                            WHERE pt."date" = (SELECT CURRENT_DATE - INTEGER '1' AS yesterday_date)
                            GROUP BY pt.artist_id, a."name" 
                            ORDER BY artists_num DESC
                            LIMIT 5"""

    engine.execute(query_top_genres)
    engine.execute(query_top_tracks)
    engine.execute(query_top_artists)

if __name__ == "__main__":
    load_view_top_five()
