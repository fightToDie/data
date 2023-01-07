from kafka import KafkaConsumer 
from json import loads
import time

import happybase
import uuid



CDH6_HBASE_THRIFT_VER='0.92'
hbase_cnxn = happybase.Connection(
    host='192.168.56.101', port=9090,
    table_prefix_separator=b'_',
    timeout=None,
    autoconnect=True,
    transport='framed',
    protocol='compact'
)
data = {
                    "artist_name": "Missy Elliott", 
                    "track_uri": "spotify:track:0UaMYEvWZi0ZqiDOoHU3YI", 
                    "artist_uri": "spotify:artist:2wIVse2owClT7go1WT98tk", 
                    "track_name": "Lose Control (feat. Ciara & Fat Man Scoop)", 
                    "album_uri": "spotify:album:6vV5UrXcfyQD1wu4Qo2I9K", 
                    "album_name": "The Cookbook"
                }

print("start")
tableName = "spotifytest"
try:
    hbase_cnxn.create_table(
    tableName, {"cf2": dict()}  
    )
except:
    pass
table = hbase_cnxn.table(tableName)
batch = table.batch(batch_size = 1000)
kafka_host = "localhost"
kafka_port = "9092"
consumer=KafkaConsumer("spotify", 
                        bootstrap_servers=['127.0.0.1:9092'], 
                        auto_offset_reset="earliest",
                        auto_commit_interval_ms=10,
                        enable_auto_commit=True, 
                        group_id='kakao-group', 
                        value_deserializer=lambda x: loads(x.decode('utf-8')), 
                        consumer_timeout_ms=1000 
            )
cnt = 0

while True:
    time.sleep(0.5)
    for message in consumer:
        if cnt % 1000 ==0: print(cnt)
        
        value=message.value
        for data in value:
            batch.put(str(uuid.uuid4()), {
                "cf2:pos".encode("utf-8"): str(data["pos"]).encode("utf-8"),
                "cf2:artist_name".encode("utf-8"): data["artist_name"].encode("utf-8"),
                "cf2:track_uri".encode("utf-8"): data["track_uri"].encode("utf-8"),
                "cf2:artist_uri".encode("utf-8"): data["artist_uri"].encode("utf-8"),
                "cf2:track_name".encode("utf-8"): data["track_name"].encode("utf-8"),
                "cf2:album_uri".encode("utf-8"): data["album_uri"].encode("utf-8"),
                "cf2:duration_ms".encode("utf-8"): str(data["duration_ms"]).encode("utf-8"),
                "cf2:album_name".encode("utf-8"): data["album_name"].encode("utf-8")  
            })
        cnt +=1
        batch.send()
        consumer.commit()
