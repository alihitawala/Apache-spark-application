import org.apache.spark.Partitioner;

/**
 * Created by aliHitawala on 10/12/16.
 */
public class CustomPartitioner extends Partitioner {
    public int partitions;

    public CustomPartitioner(int partitions) {
        this.partitions = partitions;
    }

    @Override
    public int numPartitions() {
        return this.partitions;
    }

    @Override
    public int getPartition(Object o) {
        String v = (String) o;
        return Integer.parseInt(v) % this.partitions;
    }
}
