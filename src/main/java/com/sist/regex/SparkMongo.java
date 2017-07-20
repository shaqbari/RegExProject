package com.sist.regex;

import java.io.FileReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;


public class SparkMongo implements Serializable{//메모리 이용하기 때문에 필요
	public static void main(String[] args) {
		SparkMongo sm=new SparkMongo();
		sm.mongoInsert();
		
		
	}
	
	public void mongoInsert(){
		try {
			SparkSession spark=SparkSession.builder().master("local").appName("spark_mongo")
					.config("spark.mongodb.input.uri", "mongodb://211.238.142.104/mydb.apache")//포트번호 27017아니면 써야한다.
					.config("spark.mongodb.output.uri", "mongodb://211.238.142.104/mydb.apache")
					.getOrCreate();
			
			JavaSparkContext jsc=new JavaSparkContext(spark.sparkContext());
			
			Map<String, String> option=new HashMap<String, String>();
			option.put("collection", "apache");
			WriteConfig config=WriteConfig.create(jsc).withOptions(option);
			
			FileReader fr=new FileReader("/home/sist/apache/part-00000");
			String data="";
			int i=0;
			while ((i=fr.read())!=-1) {
				data+=String.valueOf((char)i);
			}
			fr.close();
			
			data=data.replace("(", "");
			data=data.replace(")", "");
			String[] temp=data.split("\n");
			List<MyVO> list=new ArrayList<MyVO>();
			for (int a = 0; a < temp.length; a++) {
				StringTokenizer st=new StringTokenizer(temp[a], ",");
				MyVO vo=new MyVO();
				vo.setIp(st.nextToken());
				vo.setCount(Integer.parseInt(st.nextToken()));
				list.add(vo);
			}
			
			JavaRDD<Document> sd=jsc.parallelize(list).map(new Function<MyVO, Document>() {

				public Document call(MyVO vo) throws Exception {
					
					return new Document().parse("{ip:'"+vo.getIp()+"', count:"+vo.getCount()+"}");
				}
			});
			
			MongoSpark.save(sd, config);
			jsc.close();
			
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
	}
	
}
