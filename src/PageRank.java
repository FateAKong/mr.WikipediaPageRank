import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import javax.naming.ldap.Control;
import javax.print.attribute.standard.NumberOfInterveningJobs;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 8:42 PM
 */
public class PageRank {
    public static void main(String[] args) throws Exception {
        new RankInitializer("input", "results/PageRank.iter0.out_").getJob().waitForCompletion(false);

        PropertyAnalyzer analyzer = new PropertyAnalyzer("results/PageRank.iter0.out_", "results/PageRank.n.out");
        analyzer.getJob().waitForCompletion(false);

        FileSystem fs = FileSystem.get(analyzer.getJob().getConfiguration());
        // read in graph properties
        Scanner scanner = new Scanner(fs.open(new Path("results/PageRank.n.out/part-r-00000")));
        int nPages = Integer.parseInt(scanner.nextLine().substring(2));
        System.out.println("total pages: " + nPages);
        scanner.close();

        for (int i = 0; i < 2; i++) {
            new RankCalculator("results/PageRank.iter"+i+".out_", "results/PageRank.iter"+(i+1)+".out").getJob().waitForCompletion(false);
            FSDataInputStream inputStream = fs.open(new Path("tmp/sink"));
            double prevSinkRank = inputStream.readDouble();
            System.out.println("missing rank mass: " + prevSinkRank);
            inputStream.close();
            new RankFinalizer("results/PageRank.iter"+(i+1)+".out", "results/PageRank.iter"+(i+1)+".out_", prevSinkRank/nPages).getJob().waitForCompletion(false);
        }
        System.out.println("end");
        System.exit(0);
    }
}