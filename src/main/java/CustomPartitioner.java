import org.apache.spark.Partitioner;

/**
 * Created by aliHitawala on 10/12/16.
 */
public class CustomPartitioner extends Partitioner {
    public static final int NUM_PARTITION = 100;
    @Override
    public int numPartitions() {
        return NUM_PARTITION;
    }

    @Override
    public int getPartition(Object o) {
        String v = (String) o;
        return Integer.parseInt(v) % NUM_PARTITION;
    }
}
