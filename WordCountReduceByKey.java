package cn.edu.ecnu.spark.example.java.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCountReduceByKey {

    public static void run(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCountJava");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);


        JavaRDD<String> words =
                lines.flatMap(
                        new FlatMapFunction<String, String>() {
                            @Override
                            public Iterator<String> call(String line) throws Exception {
                                return Arrays.asList(line.split(" ")).iterator();
                            }
                        });

        JavaPairRDD<String, Integer> pairs =
                words.mapToPair(
                        new PairFunction<String, String, Integer>() {
                            @Override
                            public Tuple2<String, Integer> call(String word) throws Exception {
                                return new Tuple2<String, Integer>(word, 1);
                            }
                        });

        JavaPairRDD<String, Integer> wordCounts =
                pairs.reduceByKey(
                        new Function2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer t1, Integer t2) throws Exception {
                                return t1 + t2;
                            }
                        });

        wordCounts.saveAsTextFile(args[1]);
        sc.stop();
    }

    public static void main(String[] args) {
        run(args);
    }
}
