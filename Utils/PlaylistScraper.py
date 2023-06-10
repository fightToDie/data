import spotipy
from spotipy.oauth2 import SpotifyOAuth
import json
import re

SCOPE = ['user-library-read',
         'user-follow-read',
         'user-top-read',
         'playlist-read-private',
         'playlist-read-collaborative',
         'playlist-modify-public',
         'playlist-modify-private']
#USER_ID = ''
REDIRECT_URI = ''
CLIENT_ID = ''
CLIENT_SECRET = ''
auth_manager = SpotifyOAuth(
    scope=SCOPE,
    #username=USER_ID,
    redirect_uri=REDIRECT_URI,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET)

def get_categories():
    try:
        sp = spotipy.Spotify(auth_manager=auth_manager)
        query_limit = 50
        categories = []
        new_offset = 0

        while True:
            results = sp.category_playlists(category_id='hiphop', limit=query_limit, country='US', offset=new_offset)
            #results는 플레이리스트 정보만 읽어온다.

            for item in results['playlists']['items']:
                if (item is not None and item['name'] is not None):
                    # ['https:', '', 'api.spotify.com', 'v1', 'playlists', '37i9dQZF1DX0XUsuxWHRQd', 'tracks']
                    tokens = re.split(r"[\/]", item['tracks']['href'])

                    categories.append({
                        'id': item['id'],
                        'name': item['name'],
                        'url': item['external_urls']['spotify'],
                        'tracks': item['tracks']['href'],
                        'playlist_id': tokens[5],
                        'type': item['type']
                    })
            new_offset = new_offset + query_limit
            next = results['playlists']['next']
            if next is None:
                break
        return categories
    except Exception as e:
        print('Failed to upload to call get_categories: ' + str(e))

def get_songs(categories):
    try:
        sp = spotipy.Spotify(auth_manager=auth_manager)
        songs = []

        for category in categories:
            if category is None:
                break
            playlist_id = category['playlist_id']
            results = sp.playlist(playlist_id=playlist_id)
            print(results)
            exit(0)
            #플리에 따른 트랙을 읽어오는 부분
            for item in results['tracks']['items']:
                if (item is not None and item['track'] is not None and item['track']['id'] is not None and
                        item['track']['name'] is not None and item['track']['external_urls']['spotify'] is not None):
                    songs.append({
                        'id': item['track']['id'],
                        'name': item['track']['name'],
                        'url': item['track']['external_urls']['spotify']
                    })
                else:
                    break
        return songs
    except Exception as e:
        print('Failed to upload to call get_songs: ' + str(e))

categories = get_categories()
songs = get_songs(categories)
print(json.dumps(songs))
