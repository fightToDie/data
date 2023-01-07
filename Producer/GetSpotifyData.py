import os
from kafka import KafkaProducer
from datetime import datetime
import json
from multiprocessing import Pool
import threading
import time
class DataLoader(threading.Thread):
    def __init__(self, data):
        super().__init__()
        self.producer=KafkaProducer(
            acks=0, 
            compression_type='gzip',
            bootstrap_servers=["127.0.0.1:9092"],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            linger_ms=1000

          )
        self.fileList = data
        self.dataPath = "C:/Users/sudo/Desktop/spotify_million_playlist_dataset/data/"
        
    def run(self):
        print("start")
        
        for filename in self.fileList:
            start = time.time()
            print(filename)
            with open(self.dataPath + filename, 'r') as file:
                data = json.load(file)
            
            for playlist in data["playlists"]:
                getSongList = playlist["tracks"]
                self.sendData(getSongList)
            end = time.time()
            print(f"{end - start:.2f} sec")
                    
    
    def dataLoadByFile(self, filename):
        return json.load(filename)
        

    def sendData(self, data):
        self.producer.send("spotify",value=data)
        self.producer.flush()
        