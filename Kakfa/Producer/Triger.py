from GetSpotifyData import DataLoader
import os

class Triger:

    def __init__(self):
        self.div=10000
        self.fileList=[]
        self.dataPath = "C:/Users/sudo/Desktop/spotify_million_playlist_dataset/data/"
        
        
    def start(self):
        self.getFileList()
        threadList = []
        for i in range(0,len(self.fileList),100):
            t  = DataLoader(self.fileList[i:i+100])
            t.start()
        for t in threadList:
            t.join()
            print("finish")
            
            
    def getFileList(self):
        self.fileList= os.listdir(self.dataPath)
        


triger = Triger()
triger.start()
