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
 * A series of MapReduce job generating sorted page rank of Wikipedia pages
 */
public class PageRank {
    public static void main(String[] args) throws Exception {
        // Job 1: extract WikiLinks and remove red links
        new InlinkGenerator("input", "tmp/PageRank.inlink.out").getJob().waitForCompletion(false);

        // Job 2: generate outlink adjacency graph
        new OutlinkGenerator("tmp/PageRank.inlink.out", "results/PageRank.outlink.out").getJob().waitForCompletion(false);

        // Job 3: compute total number of pages denoted as N
        Job analyzer = new PropertyAnalyzer("results/PageRank.outlink.out", "results/PageRank.n.out").getJob();
        analyzer.waitForCompletion(false);
        FileSystem fs = FileSystem.get(analyzer.getConfiguration());
        Scanner scanner = new Scanner(fs.open(new Path("results/PageRank.n.out/part-r-00000")));
        int nPages = Integer.parseInt(scanner.nextLine().substring(2));
        System.out.println("total pages: " + nPages);
        scanner.close();

        // Job 4: perform PageRank calculation
        new RankInitializer("results/PageRank.outlink.out", "results/PageRank.iter0.out", nPages).getJob().waitForCompletion(false);
        for (int i = 0; i < 2; i++) {
            new RankCalculator("results/PageRank.iter"+i+".out", "results/PageRank.iter"+(i+1)+".raw.out", nPages).getJob().waitForCompletion(false);
            FSDataInputStream inputStream = fs.open(new Path("tmp/sink"));
            double prevSinkRank = inputStream.readDouble();
            System.out.println("missing rank mass: " + prevSinkRank);
            inputStream.close();
            new RankFinalizer("results/PageRank.iter"+(i+1)+".raw.out", "results/PageRank.iter"+(i+1)+".out", nPages, prevSinkRank).getJob().waitForCompletion(false);
        }
        System.out.println("end");
        System.exit(0);
    }
}