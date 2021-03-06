/**
 * Created by aliHitawala on 10/12/16.
 */
public final class PageRankQuestion2 {

    public static void main(String[] args) throws Exception {
        int numPartition = 20;
        if (args.length < 2) {
            System.err.println("Usage: PageRankQuestion2 <file> <number_of_iterations>");
            System.exit(1);
        }
        if (args.length == 3) {
            numPartition = Integer.parseInt(args[2]);
        }
        PageRankUtil.runAlgorithm(args[0], Integer.parseInt(args[1]), "CS-838-Assignment2-PartA-Question2_"+numPartition, true, false, numPartition);
    }
}
