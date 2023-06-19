import spotipy
from spotipy.oauth2 import SpotifyOAuth
import json, re, os, sys
import configparser
from kafka import KafkaProducer
from PlaylistProducer import MessageProducer
from datetime import datetime

SCOPE = ['user-library-read',
         'user-follow-read',
         'user-top-read',
         'playlist-read-private',
         'playlist-read-collaborative',
         'playlist-modify-public',
         'playlist-modify-private']

properties = configparser.ConfigParser()
properties.read('../config.ini')

USER_ID = properties.get('CONFIG', 'USER_ID')
REDIRECT_URI = properties.get('CONFIG', 'REDIRECT_URI')
CLIENT_ID = properties.get('CONFIG', 'CLIENT_ID')
CLIENT_SECRET = properties.get('CONFIG', 'CLIENT_SECRET')

auth_manager = SpotifyOAuth(
    scope=SCOPE,
    #username=USER_ID,
    redirect_uri=REDIRECT_URI,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET)

schema = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "playlists": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string"
          },
          "collaborative": {
            "type": "string"
          },
          "pid": {
            "type": "integer"
          },
          "modified_at": {
            "type": "integer"
          },
          "num_tracks": {
            "type": "integer"
          },
          "num_albums": {
            "type": "integer"
          },
          "num_followers": {
            "type": "integer"
          },
          "tracks": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "pos": {
                  "type": "integer"
                },
                "artist_name": {
                  "type": "string"
                },
                "track_uri": {
                  "type": "string"
                },
                "artist_uri": {
                  "type": "string"
                },
                "track_name": {
                  "type": "string"
                },
                "album_uri": {
                  "type": "string"
                },
                "duration_ms": {
                  "type": "integer"
                },
                "album_name": {
                  "type": "string"
                }
              },
              "required": [
                "pos",
                "artist_name",
                "track_uri",
                "artist_uri",
                "track_name",
                "album_uri",
                "duration_ms",
                "album_name"
              ]
            }
          }
        },
        "required": [
          "name",
          "collaborative",
          "pid",
          "modified_at",
          "num_tracks",
          "num_albums",
          "num_followers",
          "tracks"
        ]
      }
    }
  },
  "required": ["playlists"]
}

#Collector Section
#카테고리로 플리 검색
def get_categories(search_category):
    try:
        sp = spotipy.Spotify(auth_manager=auth_manager)
        query_limit = 50
        categories = []
        new_offset = 0

        while True:
            results = sp.category_playlists(category_id=search_category, limit=query_limit, country='US', offset=new_offset)
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

#제목으로 플리 검색
def get_playlists_with_keyword(keyword):
    playlists = []
    offset = 0
    total = 0
    cnt = 0

    while True:
        results = sp.search(q=keyword, type='playlist', limit=50, offset=offset)
        items = results['playlists']['items']
        playlists.extend(items)
        total += len(items)
        offset += len(items)

        if len(items) < 50:
            break

    return playlists

#플리 노래 목록 검색
def get_songs(categories):
    #try:
        sp = spotipy.Spotify(auth_manager=auth_manager)
        playlists = []
        playlist_id_set = set()

        for category in categories:
            if category is None:
                break
            playlist_id = category['id']

            results = sp.playlist(playlist_id=playlist_id)
            pos = 0
            tracks = []
            album_ids = set()
            modified_at = '2006-04-23T00:00:00Z'

            if results['id'] in playlist_id_set:
                continue

            #플리에 따른 트랙을 읽어오는 부분
            for item in results['tracks']['items']:
                if (item is not None and item['track'] is not None and item['track']['id'] is not None and
                        item['track']['name'] is not None and item['track']['external_urls']['spotify'] is not None):
                    album_ids.add(item['track']['album']['id'])

                    if modified_at < item['added_at']:
                        modified_at = item['added_at']

                    tracks.append({
                        'pos': pos,
                        'artist_name': item['track']['album']['artists'][0]['name'],
                        'track_uri': item['track']['uri'],
                        'artist_uri': item['track']['album']['artists'][0]['uri'],
                        'track_name': item['track']['name'],
                        'album_uri': item['track']['album']['uri'],
                        'duration_ms': item['track']['duration_ms'],
                        'album_name': item['track']['album']['name']
                    })
                    pos += 1
                else:
                    break

            playlists.append({
                'name': results['name'],
                'collaborative': results['collaborative'],
                'pid': results['id'],
                'modified_at': int(datetime.strptime(modified_at, '%Y-%m-%dT%H:%M:%SZ').timestamp()),
                'num_tracks': results['tracks']['total'],
                'num_albums': len(album_ids),
                'num_followers': results['followers']['total'],
                'tracks': tracks
            })

        return playlists
    #except Exception as e:
        #print('Failed to upload to call get_songs: ' + str(e))

#기존 Producer 자리

sp = spotipy.Spotify(auth_manager=auth_manager)

#search_category = sys.argv[1]
search_category = 'hiphop'

print("Running PlaylistScraper.py " + search_category)

categories = get_playlists_with_keyword(search_category)
playlists = get_songs(categories)

print("Found " + str(len(playlists)) + " playlists.")

#kafka producing
broker = properties.get('CONFIG', 'BROKER')
topicname = properties.get('CONFIG', 'TOPIC')
#switch to 0th arg of sys
topic = search_category
message_producer = MessageProducer(broker, topicname)

#for i in range(len(playlists)):
message_producer.send_message(playlists[0])

for i in range(len(playlists)):
    message_producer.send_message(playlists[i])
    #즉시, 발행
    message_producer.producer.flush()

"""
#json file generator
output_file_name = search_category
output_file_path = os.path.join('D:', output_file_name + '.json')

with open(output_file_path, 'w') as file:
    json.dump({'playlists': playlists}, file)
"""

#validate(instance = json.loads(playlists), schema = schema)