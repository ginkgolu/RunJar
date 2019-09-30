package com.geostar.geosmarter.wordcount;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 对单词个数进行统计，然后按照出现次数由大到小输出。
 * 
 * @useage: WordCountMain
 * 
 * @author luyinxing
 * @since 2019-09-24
 */
public class WordCountMain {

	static final Logger logger = Logger.getLogger(WordCountMain.class);
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		//Step 1:初始化，创建会话
		SparkConf conf = new SparkConf();
		conf.setMaster("local");//spark运行模式
		conf.setAppName("wordCount");//application名称
		JavaSparkContext sc = new JavaSparkContext(conf);// JavaSparkContext是通往集群的唯一通道
		sc.setCheckpointDir("RunJar/src/main/resources/checkpointDir");

		//Step 2:读取文本文件，生成第一个RDD
		JavaRDD<String> lines = sc.textFile("RunJar/src/main/resources/word_count_test_data.txt");
		
		//Step 3：filter操作，过滤数据，只保留包含Hello和Hello1的数据
		JavaRDD<String> filterLines = lines.filter(
			new Function<String, Boolean>() {
				private static final long serialVersionUID = -7222805379226673309L;
				@Override
				public Boolean call(String line) throws Exception {
					return line.contains("Hello") || line.contains("Hello1");
				}
		});

		//Step 4:flagMap操作，将每行数据以制表符分隔，扁平化处理
		JavaRDD<String> words = filterLines.flatMap(
			new FlatMapFunction<String, String>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<String> call(String line) {
					return Arrays.asList(line.split("\t")).iterator();
				}
		});

		//Step 5:mapToPair操作，将String转为<String,Integer>
		JavaPairRDD<String, Integer> pairWords = words.mapToPair(
			new PairFunction<String, String, Integer>() {
				private static final long serialVersionUID = -2962565084610481112L;
				@Override
				public Tuple2<String, Integer> call(String word) {
					return new Tuple2<>(word, 1);
				}
		});
		
		//Step 6:reduceByKey操作，相同的key，将其value相加
		JavaPairRDD<String, Integer> reduce = pairWords.reduceByKey(
			new Function2<Integer, Integer, Integer>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Integer call(Integer v1, Integer v2) {
					return v1 + v2;
				}
		});
		
		
		//Step 7:mapToPair操作，将key和value颠倒
		JavaPairRDD<Integer, String> result = reduce.mapToPair(
			new PairFunction<Tuple2<String, Integer>, Integer, String>() {
				private static final long serialVersionUID = -5424248599737135001L;
				@Override
				public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) {
					return tuple.swap();
				}
		});
		
		//Step 8:sortByKey操作，升序true默认，false降序
		JavaPairRDD<Integer, String> sortByKeyResult = result.sortByKey(false);
		
		//Step 9:打印结果
		sortByKeyResult.foreach(new VoidFunction<Tuple2< Integer, String>>() {
			private static final long serialVersionUID = 2496749255383918020L;
			@Override
			public void call(Tuple2<Integer, String> tuple) {
				logger.info(tuple.swap());
			}
		});

		//Step 10:缓存数据
		sortByKeyResult.cache();
		sortByKeyResult.checkpoint();
		
		//Step 11:统计
		long count = sortByKeyResult.count();
		logger.info("Count : " + count);

		//Step 12:collect操作，将worker端的结果回收到Driver端，如果结果集过大，会导致Driver内存溢出
		List<Tuple2<Integer, String>> collectResult = sortByKeyResult.collect();
		for (Tuple2<Integer, String> tem : collectResult) {
			logger.info("collect : " + tem.swap());
		}
		
		//Step 13:关闭会话
		sc.stop();

		long end = System.currentTimeMillis();
		logger.info("total: " + (end - begin)/1000 + " s");
	}
	
}