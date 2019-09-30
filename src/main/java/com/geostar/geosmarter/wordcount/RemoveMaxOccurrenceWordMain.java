package com.geostar.geosmarter.wordcount;

import java.io.Serializable;
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

import com.geostar.geosmarter.traffic.datagenerate.Common;

import scala.Tuple2;


/**
 * 需求： 读取一个文件，统计单词出现的次数。并且把出现次数多的单词移除掉，然后将剩下的单词按照出现次数降序排序。
 * 
 * @author luyinxing
 * @since 2019-09-24
 */
public class RemoveMaxOccurrenceWordMain implements Serializable {

	private static final long serialVersionUID = 1L;

	static final Logger logger = Logger.getLogger(RemoveMaxOccurrenceWordMain.class);

	public static void main(String[] args) {
		//Step 1:创建会话
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName("Remove More Occurrence Word");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		//Step 2:读取数据，生成第一个RDD
		JavaRDD<String> lines = jsc.textFile("RunJar/src/main/resources/remove_max_occurrence_test_data.txt");

		//Step 3:flatMap操作，将每行数据以空格切分，并扁平化处理
		JavaRDD<String> wordsRDD = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<String> call(String line) {
				return Arrays.asList(line.split(Common.BLANK)).iterator();
			}
		});

		//Step 4:mapToPair操作，将String转为String，Integer类型，每个单词计数为1
		JavaPairRDD<String, Integer> wordWithValueRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(String word) {
				return new Tuple2<>(word, 1);
			}
		});

		//Step 5:reduceByKey操作，相同的key将其value相加
		JavaPairRDD<String, Integer> wordWithTotalNumRDD = wordWithValueRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) {
				return v1 + v2;
			}
		});

		//Step 6:mapToPair操作，将key与value互换位置
		JavaPairRDD<Integer, String> totalNumAndWordRDD = wordWithTotalNumRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) {
				return tuple2.swap();
			}
		});

		//Step 7:sortByKey操作，默认正序true
		JavaPairRDD<Integer, String> totalNumAndWordSortByKeyRDD = totalNumAndWordRDD.sortByKey(false);

		//Step 8:RDD缓存
		JavaPairRDD<Integer, String> totalNumAndWordSortByKeyCachedRDD = totalNumAndWordSortByKeyRDD.cache();

		//Step 9:取出第一个值，即出现最多的单词
		List<Tuple2<Integer, String>> takeOne = totalNumAndWordSortByKeyCachedRDD.take(1);
		final String oneWord = takeOne.get(0)._2;
		Integer oneWordTotalNum = takeOne.get(0)._1;
		logger.info("Find the Max Occurrence Word : " + oneWord + ", Number : " + oneWordTotalNum);

		//Step 10：过滤，去掉出现次数最多的单子
		JavaPairRDD<Integer, String> resultRDD = totalNumAndWordSortByKeyCachedRDD.filter(new Function<Tuple2<Integer, String>, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Tuple2<Integer, String> tuple2) {
				// 移除掉出现次数最多的单词
				return !tuple2._2.equals(oneWord);
			}
		});

		//Step 11:将key和value互换并输出
		resultRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<Integer, String> tuple2) {
				logger.info(tuple2.swap());
			}
		});

		//Step 12:关闭会话
		jsc.stop();
	}

}
