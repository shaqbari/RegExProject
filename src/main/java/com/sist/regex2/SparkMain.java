package com.sist.regex2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

//mongodb버전이 잘 맞아야 한다.
public class SparkMain implements Serializable{ //참조변수때문에 Serializable을 줘야함
	public static void main(String[] args) {
		SparkMain sm=new SparkMain();
		sm.execute();
	}
	
	public void execute(){
		try {
			SparkConf conf=new SparkConf().setAppName("RegEx").setMaster("local");
			JavaSparkContext sc=new JavaSparkContext(conf);
			JavaRDD<String> files=sc.textFile("/home/sist/access_log");		
			/*
			[찾는문자열] {횟수}
			[알파벳] => [A-Z][a-z] [A-Za-z] => ^ $
			^[A-Z] => [a-z]$ a$ => [abc]*$   //*은 들어오지 않을수도 있다는 뜻 , 즉 [a]* 는 a나 aaa를 찾는 방법, [a]+ 는 aa, aaa, aaa를 찾아냄
 			[A-Z] 알파벳중에 한글자 들어옴  
 			^[A-Z]* 알파벳 뒤에 단어가 붙던지 안붙던지 상관없음
 			^[A-Z]+ 알파벳 뒤에 반드시 한글지 붙어야함
 			^[A-Za-z]{3} 알파벳세글자가져옴
			
			[한글] => 
			[가-힣]$한글로 끝남
			^[가-힣] 한글로 시작함
			[^가-힣] 한글을 제거함
			[^가-힣]+ 한글로 된 단어들을 제거함
			
			[숫자] =>[0-9]{1,3} 
			.은 모든 것
			\\. d{} 정수를 나타냄
			
			*/
			//64.242.88.10
			final String regex="[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}"; //ip서치
			//final String regex="(([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3}))"; //ip서치
			
			// 07/Mar/2004
			//final String regex="[0-9]{2}/[A-Za-z]{3}/[0-9]{4}";
			final Pattern p=Pattern.compile(regex);
			JavaRDD<String> words=files.flatMap(new FlatMapFunction<String, String>() {//전체, 한줄
				//List<String> list=new ArrayList<String>(); 이버전에서는 여기있으면 누적되어버린다.

				public Iterator<String> call(String s) throws Exception {//한줄
					 List<String> list=new ArrayList<String>(); 
					 Matcher m=p.matcher(s);
					 if(m.find()){
						 list.add(m.group()); //패턴이 아니라 찾은 문자열 전체를 가져옴
					 }
					 return list.iterator(); //spakr2.0에서는 interator()로 보내야함
				}
			});
			JavaPairRDD<String, Integer> counts=words.mapToPair(new PairFunction<String, String, Integer>() {
				public Tuple2<String, Integer> call(String s) throws Exception {
					return new Tuple2<String, Integer>(s, 1);
				}
			});
			//누적
			JavaPairRDD<String, Integer> reduce=counts.reduceByKey(new Function2<Integer, Integer, Integer>() {
				@Override
				public Integer call(Integer sum, Integer i) throws Exception {
					return sum+i;
				}
			});
			reduce.saveAsTextFile("/home/sist/apache19");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
