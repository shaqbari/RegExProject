package com.sist.regex;

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

public class SparkMain implements Serializable{//참조에서 가져올때 반드시 붙여야 한다.
	public static void main(String[] args) {
		SparkMain sm=new SparkMain();
		sm.execute();
	}
	
	public void execute(){
		try {
			SparkConf conf=new SparkConf().setAppName("RegEx").setMaster("local");
			JavaSparkContext sc=new JavaSparkContext(conf);
			JavaRDD<String> files=sc.textFile("/home/sist/access_log");
			//파일읽을때JavaRDD 외부읽을때 JavaDStream
			
			//[찾는 문자열]{횟수}
			//[알파벳]=> [A-Z][a-z][A-Za-z][A-z] => ^시작   $마지막
			/*^[A-z] 대문자로 시작하는; [a-z]$ 소문자로 끝나는; [a]$ a로 끝나는; [abc]*$ *(0~여러번)은 들어오지 않을수도 있다.
			 * 
			 * [a]* 앞문자가 0번이상  ; [a]+ 앞문자가 1번이상 ; [a]? 앞문자가 0~1
			 * 
			 * ^[A-Z]{3} 알파벳3개로 시작하는
			 * 
			 * 
			 * */
			
			//[한글] => [가-힣]$ 한글로 끝남 ; ^[가-힣]한글로 시작; [^가-힣]한글이 아닌
			/*
			 * [가-힣]+
			 * 
			 * 
			//[숫자] => [0~9]{1,3} 한자리에서 세자리 숫자 
			/*
			 * ?
			 * .은 임의의 한문자(1)
			 * \\.은 .자체
			 * 			
			 * 
			 * 
			 * 
			 * */
			//64.242.88.10
			//final String regex="[0~9]{1,3}\\.[0~9]{1,3}\\.[0~9]{1,3}\\.[0~9]{1,3}";물결이아니다.
			final String regex="[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}";
			
			// 07/Mar/2004
			//final String regex="[0~9]{2}\\/[A-Za-z]{3}\\/[0~9]{4}";
			//final String regex="[0-9]{2}\\/[A-Za-z]{3}\\/[0-9]{4}";
			
			final Pattern p=Pattern.compile(regex);
																	//전체		한줄
			JavaRDD<String> words=files.flatMap(new FlatMapFunction<String, String>() {
				
				//List<String> list=new ArrayList<String>(); //여기있으면 누적되어 버린다.
				public Iterator<String> call(String s) throws Exception {
					List<String> list=new ArrayList<String>();
					Matcher m=p.matcher(s);
					
					// 한줄에 한개만 있으므로 여기서는 while을 안써도 된다.
					if(m.find()) {
						System.out.println(m.group());
						list.add(m.group());
					}
					
					//return형이 Iterable이 아닐 때는 아래와 같이 써야 한다.
					return list.iterator();
				}
			});
			
			System.out.println("들어오나?");
			JavaPairRDD<String, Integer> counts=words.mapToPair(new PairFunction<String, String, Integer>() {

				public Tuple2<String, Integer> call(String s) throws Exception {

					return new Tuple2<String, Integer>(s, 1);
				}
			});					
			
			JavaPairRDD<String, Integer> reduce=counts.reduceByKey(new Function2<Integer, Integer, Integer>() {
				
				public Integer call(Integer sum, Integer i) throws Exception {
					return sum+i;
				}
			});
			
			//reduce.print() Stream에서만 나온다.
			
			reduce.saveAsTextFile("/home/sist/apache21");
			
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
	}
}