package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        SparkConf sparkConf=new SparkConf().setAppName("TP ventes").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sc.textFile("ventes.txt");
        JavaPairRDD<String, Double> rddVente = rdd.mapToPair(new PairFunction<String, String, Double>() {

            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                String[] strings = s.split(" ");
                String ville = strings[1];
                Double price = Double.parseDouble(strings[3]);
                String date[]=strings[0].split("-");
                String year=date[2];
                return new Tuple2<>(ville+","+year, price);
            }
        });
        List<Tuple2<String, Double>> ventes = rddVente.reduceByKey((aDouble, aDouble2) -> aDouble + aDouble2).collect();

        for (Tuple2<String,Double> v:ventes) {
            System.out.println(v._1+" "+v._2);
        }
    }
}