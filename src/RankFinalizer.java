import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 1:04 PM
 */
public class RankFinalizer {

    private static final double DAMPING_FACTOR = 0.85;
    private static final String sinkPath = "s3n://fate.akong/tmp/sink";
    private Job job = null;
    private static double prevAvgSinkRank;

    public Configuration getConfig() {
        return job.getConfiguration();
    }

    public RankFinalizer(String input, String output, String property) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        job = new Job(config, "RankFinalizer");

        job.setJarByClass(RankFinalizer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageWritable.class);

        job.setMapperClass(Map.class);
//        job.setCombinerClass(Combine.class);
//        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(PageInputFormat.class);
        job.setOutputFormatClass(PageOutputFormat.class);
//        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        Path outputPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outputPath);
        FileSystem fs = FileSystem.get(outputPath.toUri(), config);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        // read in graph properties
        FSDataInputStream inputStream = null;
        inputStream = fs.open(new Path(property));
        String properties = inputStream.readUTF();
        inputStream.close();
        int nPages = Integer.parseInt(properties.substring(2));
        inputStream = fs.open(new Path(sinkPath));
        prevAvgSinkRank = inputStream.readDouble()/nPages;
        inputStream.close();
    }

    private static class Map extends Mapper<Text, PageWritable, Text, PageWritable> {
        @Override
        protected void map(Text key, PageWritable value, Context context) throws IOException, InterruptedException {
            double rank = 1 - DAMPING_FACTOR + DAMPING_FACTOR * (value.getRank()+prevAvgSinkRank);
            context.write(key, new PageWritable(rank, value.getOutlinks()));
        }
    }


}