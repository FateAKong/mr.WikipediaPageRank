import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * An auxiliary MapReduce procedure for RankCalculator iterations used to handle dead-ends (sink pages)
 */
public class RankFinalizer {

    private static final double DAMPING_FACTOR = 0.85;
    private static double nPages;
    private static double prevSinkRank;
    private Job job = null;

    public Job getJob() {
        return job;
    }

    public RankFinalizer(String input, String output, int nPages, double prevSinkRank) throws IOException, InterruptedException, ClassNotFoundException {
        RankFinalizer.nPages = nPages;
        RankFinalizer.prevSinkRank = prevSinkRank;

        Configuration config = new Configuration();
        job = new Job(config, "RankFinalizer");

        job.setJarByClass(RankFinalizer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageWritable.class);

        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(PageInputFormat.class);
        job.setOutputFormatClass(PageOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        Path outputPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outputPath);
        FileSystem fs = FileSystem.get(outputPath.toUri(), config);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
    }

    private static class Map extends Mapper<Text, PageWritable, Text, PageWritable> {
        @Override
        protected void map(Text key, PageWritable value, Context context) throws IOException, InterruptedException {
            // calculate rank (initialized as 1/N) for next iteration (taking sink rank into consideration)
            double rank = (1 - DAMPING_FACTOR)/nPages + DAMPING_FACTOR * (value.getRank() + prevSinkRank/nPages);
            context.write(key, new PageWritable(rank, value.getOutlinks()));
        }
    }


}