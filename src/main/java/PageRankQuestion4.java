/**
 * Created by aliHitawala on 10/12/16.
 */
public final class PageRankQuestion4 {

    public static void main(String[] args) throws Exception {
        int numPartition = 20;
        boolean isCaching = false;
        if (args.length == 3) {
            isCaching = true;
        }
        PageRankUtil.runAlgorithm(args[0], Integer.parseInt(args[1]), "CS-838-Assignment2-PartA-Question4-"+isCaching, false, isCaching, numPartition);
    }
}
