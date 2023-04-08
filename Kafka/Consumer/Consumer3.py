from kafka import KafkaConsumer 
from json import loads
import time

import happybase
import uuid

class Consumer:
    def __init__(self):
        self.conn = happybase.Connection(
                        host='192.168.56.101', port=9090,
                        table_prefix_separator=b'_',
                        timeout=None,
                        autoconnect=True,
                        transport='framed',
                        protocol='compact'
                    )
        self.consumer = KafkaConsumer("spotify", 
                        bootstrap_servers=['127.0.0.1:9092'], 
                        auto_offset_reset="earliest",
                        auto_commit_interval_ms=10,
                        enable_auto_commit=True, 
                        group_id='kakao-group', 
                        value_deserializer=lambda x: loads(x.decode('utf-8')), 
                        consumer_timeout_ms=1000 
                    )
        self.tableName = "spotifytest"
        self.table = self.conn.table(self.tableName)
        self.batchSize = 1000
        self.batch = self.table.batch(batch_size=self.batchSize)

    def createTable(self, tableName):
        try:
            self.conn.create_table(
                tableName, {"cf2": dict()}  
                )
            print("success")
        except:
            print("exist table name")
            pass 
    
    def getDataAtKafkaAndPutInHBase(self):
        cnt =0
        while True:
            time.sleep(0.5)
            print("sleep")
            for message in self.consumer:
                if cnt % 1000 ==0: print(cnt)
                
                value=message.value
                for data in value:
                    self.batch.put(str(uuid.uuid4()), {
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
                self.batch.send()
                self.consumer.commit()


consum = Consumer()
consum.getDataAtKafkaAndPutInHBase()



