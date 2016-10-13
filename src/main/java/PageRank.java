/**
 * Created by aliHitawala on 10/12/16.
 */
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 *
 * Example Usage:
 * <pre>
 * bin/run-example JavaPageRank data/mllib/pagerank_data.txt 10
 * </pre>
 */
public final class PageRank {
    private static final Pattern SPACES = Pattern.compile("\\s+");

    private static class Sum implements Function2<Double, Double, Double> {
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: PageRank <file> <number_of_iterations>");
            System.exit(1);
        }



        SparkConf conf = new SparkConf()
                .setMaster("spark://10.254.0.53:7077")
                .setAppName("CS-838-Assignment2-PartA-1")
                .set("spark.driver.memory", "1g")
                .set("spark.eventLog.enabled", "true")
                .set("spark.eventLog.dir", "hdfs:/tmp/spark-events")
                .set("spark.executor.memory", "1g")
                .set("spark.executor.cores", "4")
                .set("spark.task.cpus", "1");
        SparkSession spark = new SparkSession(SparkContext.getOrCreate(conf));

        // Loads in input file. It should be in format of:
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     ...
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        // Loads all URLs from input file and initialize their neighbors.
        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String s) {
                        String[] parts = SPACES.split(s);
                        return new Tuple2<String, String>(parts[0], parts[1]);
                    }
                }).distinct().groupByKey().cache();

        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
            public Double call(Iterable<String> rs) {
                return 1.0;
            }
        });

        // Calculates and updates URL ranks continuously using PageRank algorithm.
        for (int current = 0; current < Integer.parseInt(args[1]); current++) {
            // Calculates URL contributions to the rank of other URLs.
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
                        public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
                            int urlCount = Iterables.size(s._1);
                            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
                            for (String n : s._1) {
                                results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
                            }
                            return results.iterator();
                        }
                    });

            // Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
                public Double call(Double sum) {
                    return 0.15 + sum * 0.85;
                }
            });
        }

        // Collects all URL ranks and dump them to console.
        List<Tuple2<String, Double>> output = ranks.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
        }

        spark.stop();
    }
}
