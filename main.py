import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import sqlalchemy 
import mysql.connector

#you need your Client Id and your Client Secret from your spotify account
client_id =  'yourClientId'
client_secret = "yourClientSecret"
redirect_uri = "http://localhost:3000"

#you need the username and password for your MySQL connection, as well as the name of the database you want to connect to
db_url ='mysql+mysqlconnector://mysqlusername:mysqlpassword@localhost:3306/databasename'

def check_if_valid_data(df: pd.DataFrame):
        #transform data
        if df.empty:
                print('There\'s no songs in the Dataframe.')
                return False
        
        if not isinstance(df['track_time'], pd.Series):
                raise Exception('The primary key is not unique in  the Dataset.')
        else:
                pass

        if df.isnull().values.any():
                raise Exception('There\'s empty values in  the Dataset.')
        #check time of data


if __name__ == '__main__':
        #extract data
        track_artist = []
        track_name = []
        track_time = []

        sp = spotipy.Spotify(
                                auth_manager=SpotifyOAuth(client_id= client_id,
                                                        client_secret=client_secret,
                                                        redirect_uri= redirect_uri,
                                                        scope='user-read-recently-played')
                                )
        top_tracks = sp.current_user_recently_played(limit=50)

        for idx, item in enumerate(top_tracks['items']):

                track = item['track']
                track_artist.append(track['artists'][0]['name'])
                track_name.append(track['name'])
                track_time.append(item['played_at'])
        
        song_dict = {
                'track_name': track_name,
                'track_artist': track_artist,
                'track_time': track_time
        }

        songs_dataframe = pd.DataFrame(song_dict, columns=['track_name', 'track_artist', 'track_time'])
        print(songs_dataframe)
                
        #transform data
        if check_if_valid_data(songs_dataframe):
                print('Data can now be loaded into a Database.')


        #load data on-premise database
        engine = sqlalchemy.create_engine(db_url)
        connection = engine.connect()
        #extract data: result = pd.read_sql_query('SELECT * FROM recently_tracks', connection)
        songs_dataframe.to_sql(name= 'recently_tracks', con=engine, index= False, if_exists='append')
        connection.close

         
