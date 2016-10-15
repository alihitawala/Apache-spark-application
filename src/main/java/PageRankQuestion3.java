/**
 * Created by aliHitawala on 10/12/16.
 */
public final class PageRankQuestion3 {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: PageRankQuestion3 <file> <number_of_iterations>");
            System.exit(1);
        }
        PageRankUtil.runAlgorithm(args[0], Integer.parseInt(args[1]), "CS-838-Assignment2-PartA-Question3", true, true, 100);
    }
}
