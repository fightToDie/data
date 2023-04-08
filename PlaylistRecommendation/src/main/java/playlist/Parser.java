package playlist;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;

import org.bson.Document;

import com.mongodb.client.*;
import com.mongodb.client.result.InsertOneResult;
import com.opencsv.CSVReader;

public class Parser {
	static String URI = "mongodb://localhost:27017";
	static String DB = "test", COLLECTION = "playlist";
	static MongoDatabase db;
	
	public static MongoDatabase getMongoDatabase() {
		/*
		 * To make a connection to a running MongoDB instance, 
		 * use MongoClients.create to create a new MongoClient instance.
		 * A MongoClient instance actually represents a pool of connections to the database; 
		 * you will only need one instance of class MongoClient even with multiple concurrently executing asynchronous operations.
		 */
		MongoClient client = MongoClients.create(URI);
		
		return client.getDatabase(DB);
	}
	
	/*
	 * 도큐먼트에 pid, playlist 곡들을 담아 DB에 저장
	 * Collection을 매개변수로 받도록 재정의 필요
	 * 현재는 test DB의 playlist 컬렉션에 저장된다.
	 */
	public static void insertOnePlaylistRecommendation(String pid, List<String>playlist) {
		MongoCollection<Document> collection = db.getCollection(COLLECTION);
	
		Document doc = new Document();

		doc.append("id", pid);
		doc.append("rec", playlist);

		InsertOneResult result = collection.insertOne(doc);
		System.out.println("==> InsertOneResult : " + result.getInsertedId());
		
		return;
	}
	
	/*
	 * 여러 도큐먼트를 DB에 한번에 저장
	 * 트랜잭션 지원 여부 확인 필요
	 */
	public static void bulkInsertPlaylistRecommendation(String[] pid, List<ArrayList<String>> playlist) {
		MongoCollection<Document> collection = db.getCollection(COLLECTION);
		List<Document> insertList = new ArrayList<>();
		
		for(int i = 0; i < pid.length; i++) {
			Document document = new Document();
			document.append("id", pid[i]);
			document.append("rec", document);
			insertList.add(document);
		}
		
		try{
			collection.insertMany(insertList);
			System.out.println("[삽입 성공]");
		}catch(Exception e) {
			System.err.println("[삽입 실패]");
			e.printStackTrace();
		}
		
		return;
	}
	
	public static void main(String[] args) {
		String file = "D:\\id_uri_map.csv";
		String resFile = "D:\\cf2submit_v4-1.txt";

		HashMap<String, String> hashset = new HashMap<>();//id - pid 매핑
		db = getMongoDatabase();
		
		try(FileReader fileReader = new FileReader(file);
			FileReader resfileReader = new FileReader(resFile);
			CSVReader csvReader = new CSVReader(fileReader);
			)
		{
			String[] nextRecord;
			String line;
			BufferedReader br = new BufferedReader(resfileReader);
			
			//id - pid 복원 및 저장
			while((nextRecord = csvReader.readNext()) != null) {
				hashset.put(nextRecord[0], nextRecord[1]); //변경 pid, 원본 pid
			}
			
			while((line = br.readLine()) != null) {
				String[] map = line.split(",");
				List<String> list = new ArrayList<>();
				//System.out.println(hashset.get(String.valueOf(new BigDecimal(map[0]).intValue())));
				//pid 범위가 200만대라서 int로 충분
				//pid는 노래 원본 uri가 담긴다. 추천 알고리즘에서 쓰인 int pid를 쓸 경우, hashset.get() 부분만 trim 해준다.
				String pid = hashset.get(String.valueOf(new BigDecimal(map[0]).intValue()));
				
				for(int i = 1; i < map.length; i++) {
					String num = String.valueOf(new BigDecimal(map[i]).intValue());
					list.add(hashset.get(num));
				}
				
				insertOnePlaylistRecommendation(pid, list);
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}