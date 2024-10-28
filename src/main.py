import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import sqlalchemy 
import os
from dotenv import load_dotenv 
#import mysql.connector

load_dotenv() 
client_id =  os.getenv("client_id")
client_secret = os.getenv("client_secret")
redirect_uri = os.getenv("redirect_uri")

#or without .env file
#client_id =  'yourClientId'
#client_secret = "yourClientSecret"
#redirect_uri = "http://localhost:3000"

#you need the username and password for your MySQL connection, as well as the name of the database you want to connect to
db_url ='mysql+mysqlconnector://mysqlusername:mysqlpassword@localhost:3306/databasename'

def transformation(df: pd.DataFrame):
        if df.empty:
                return False
                
        if any((df['track_time']).duplicated(keep=False)):
                raise Exception('The primary key is not unique in the Dataset.')


        if df.isnull().values.any():
                raise Exception('There\'s empty values in  the Dataset.')       
        return True

def loading(db : str):
        try:
                engine = sqlalchemy.create_engine(db)
                connection = engine.connect()
                #result = pd.read_sql_query('SELECT * FROM recently_tracks', connection)
                songs_data.to_sql(name= 'recently_tracks', con=engine, index=False, if_exists='replace')
                connection.close()
                return True
        except:
                return False

if __name__ == '__main__':
        #
        track_artist = []
        track_name = []
        track_time = []

        sp = spotipy.Spotify(
                                auth_manager=SpotifyOAuth(client_id= client_id,
                                                        client_secret=client_secret,
                                                        redirect_uri= redirect_uri,
                                                        cache_path="token.txt",
                                                        scope='user-read-recently-played')
                                )
        top_tracks = sp.current_user_recently_played(limit=50)

        for item in top_tracks['items']:

                track = item['track']
                track_artist.append(track['artists'][0]['name'])
                track_name.append(track['name'])
                track_time.append(item['played_at'])
        
        song_dict = {
                'track_name': track_name,
                'track_artist': track_artist,
                'track_time': track_time
        }

        songs_data = pd.DataFrame(song_dict, columns=['track_name', 'track_artist', 'track_time'])
                
        #
        if transformation(songs_data):
                print('Data can now be loaded into the Database.')
                #On-Premises
                if loading(db_url):
                        print("Data has been loaded into the Database.")
                else:
                        print("There was an error connecting to the Database.")
        else:
                print("There are no songs in the DataFrame.")


