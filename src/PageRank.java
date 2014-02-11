import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
        String bucket = "s3n://" + args[0] + "/";

        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
        Path tmpPath = new Path(bucket+"tmp/");
        Path resPath = new Path(bucket+"results/");

        if (fs.exists(tmpPath)) fs.delete(tmpPath, true);
        if (fs.exists(resPath)) fs.delete(resPath, true);

        // Job 1: extract WikiLinks and remove red links
        new InlinkGenerator(bucket+"input", bucket+"tmp/PageRank.inlink.out").getJob().waitForCompletion(false);

        // Job 2: generate outlink adjacency graph
        new OutlinkGenerator(bucket+"tmp/PageRank.inlink.out", bucket+"tmp/PageRank.outlink.out").getJob().waitForCompletion(false);
        FileUtil.copyMerge(fs, new Path(bucket+"tmp/PageRank.outlink.out"), fs, new Path(bucket+"results/PageRank.outlink.out"), false, config, "");

        // Job 3: compute total number of pages denoted as N
        new PropertyAnalyzer(bucket+"tmp/PageRank.outlink.out", bucket+"tmp/PageRank.n.out").getJob().waitForCompletion(false);
        FileUtil.copyMerge(fs, new Path(bucket+"tmp/PageRank.n.out"), fs, new Path(bucket+"results/PageRank.n.out"), false, config, "");

        Scanner scanner = new Scanner(fs.open(new Path(bucket+"results/PageRank.n.out")));
        int nPages = Integer.parseInt(scanner.nextLine().substring(2));
        System.out.println("total pages: " + nPages);
        scanner.close();

        // Job 4: perform PageRank calculation
        new RankInitializer(bucket+"tmp/PageRank.outlink.out", bucket+"tmp/PageRank.iter0.out", nPages).getJob().waitForCompletion(false);
        for (int i = 0; i < 8; i++) {
            new RankCalculator(bucket+"tmp/PageRank.iter"+i+".out", bucket+"tmp/PageRank.iter"+(i+1)+".out.raw", nPages).getJob().waitForCompletion(false);
            FSDataInputStream inputStream = fs.open(new Path("tmp/sink"));
            double prevSinkRank = inputStream.readDouble();
            System.out.println("missing rank mass: " + prevSinkRank);
            inputStream.close();
            new RankFinalizer(bucket+"tmp/PageRank.iter"+(i+1)+".out.raw", bucket+"tmp/PageRank.iter"+(i+1)+".out", nPages, prevSinkRank).getJob().waitForCompletion(false);
        }

        new RankSorter(bucket+"tmp/PageRank.iter1.out", bucket+"tmp/PageRank.iter1.out.sorted", nPages).getJob().waitForCompletion(false);
        new RankSorter(bucket+"tmp/PageRank.iter8.out", bucket+"tmp/PageRank.iter8.out.sorted", nPages).getJob().waitForCompletion(false);
        FileUtil.copyMerge(fs, new Path(bucket+"tmp/PageRank.iter1.out.sorted"), fs, new Path(bucket+"results/PageRank.iter1.out"), false, config, "");
        FileUtil.copyMerge(fs, new Path(bucket+"tmp/PageRank.iter8.out.sorted"), fs, new Path(bucket+"results/PageRank.iter8.out"), false, config, "");

        System.out.println("end");
        System.exit(0);
    }
}