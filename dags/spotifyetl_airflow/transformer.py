from datetime import date
import datetime as dt

class PlayedTracksTransformer:
    def __init__(self) -> None:
        pass

    def filter_date(self, df):
        yesterday = str(date.today() - dt.timedelta(days=1))
        df = df[df["date"].str[0:10] == yesterday]

        return df

    def check_df_empty(self, df):
        if df.empty:
            print("No songs downloaded. Finishing execution")
            return False
    
    def prepare_data(self, df):
        df = self.filter_date(df)
        self.check_df_empty(df)
        df['id'].replace('[-.:]','',regex=True, inplace=True)
        df = df.drop_duplicates(subset=['id'])
        df.set_index(['id'], inplace = True, drop = True)

        return df

class TracksArtistsTransformer(PlayedTracksTransformer):
    def __init__(self) -> None:
        super().__init__()
    
    def prepare_data(self, df, before):
        df = df.rename(columns={before[0]: "id", before[1]: "name"})
        df['name'] = df['name'].str.lower()
        df = super().prepare_data(df)        
        df = df.drop(columns=["date"])

        return df