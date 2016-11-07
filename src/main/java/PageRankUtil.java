import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by aliHitawala on 10/12/16.
 */
public class PageRankUtil {
    private static final Pattern SPACES = Pattern.compile("\\s+");

    private static class Sum implements Function2<Double, Double, Double> {
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public static void runAlgorithm(String inputFile, int iterations, String appName, boolean isPartition, boolean isCaching, int numPartitions) {
        SparkConf conf = new SparkConf()
                .setMaster("spark://10.254.0.53:7077")
                .setAppName(appName)
                .set("spark.driver.memory", "8g")
                .set("spark.driver.cores", "2")
                .set("spark.eventLog.enabled", "true")
                .set("spark.eventLog.dir", "hdfs:/tmp/spark-events")
                .set("spark.executor.memory", "4g")
                .set("spark.executor.cores", "1")
                .set("spark.executor.instances","20")
                .set("spark.task.cpus", "1");
        SparkSession spark = new SparkSession(SparkContext.getOrCreate(conf));
        JavaRDD<String> lines = spark.read().textFile(inputFile).javaRDD().repartition(20);
        JavaPairRDD<String, Iterable<String>> links;
        if (isPartition) {
            if (isCaching) {
                links = lines.mapToPair(
                        new PairFunction<String, String, String>() {
                            public Tuple2<String, String> call(String s) {
                                String[] parts = SPACES.split(s);
                                return new Tuple2<String, String>(parts[0], parts[1]);
                            }
                        }).groupByKey(new CustomPartitioner(numPartitions)).persist(StorageLevel.MEMORY_ONLY());
            } else {
                links = lines.mapToPair(
                        new PairFunction<String, String, String>() {
                            public Tuple2<String, String> call(String s) {
                                String[] parts = SPACES.split(s);
                                return new Tuple2<String, String>(parts[0], parts[1]);
                            }
                        }).groupByKey(new CustomPartitioner(numPartitions));
            }
        } else {
            links = lines.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String s) {
                        String[] parts = SPACES.split(s);
                        return new Tuple2<String, String>(parts[0], parts[1]);
                    }
                }).groupByKey();
        }
        JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
            public Double call(Iterable<String> rs) {
                return 1.0;
            }
        });
        for (int current = 0; current < iterations; current++) {
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
            ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
                public Double call(Double sum) {
                    return 0.15 + sum * 0.85;
                }
            });
        }
        ranks.saveAsTextFile("hdfs:/home/ubuntu/output");
        spark.stop();
    }
}
