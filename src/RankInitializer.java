import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Parse outlink graph into ready-to-process ranked graph
 */
public class RankInitializer {

    private Job job = null;
    private static int nPages;

    public Job getJob() {
        return job;
    }

    public RankInitializer(String input, String output, int nPages) throws IOException {
        RankInitializer.nPages = nPages;
        job = new Job(new Configuration(), "RankInitializer");

        job.setJarByClass(RankInitializer.class);

//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(PageWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageWritable.class);

        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(PageOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
    }

    private static class Map extends Mapper<Text, Text, Text, PageWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
//            PageWritable page = new PageWritable(1.0/nPages, new ArrayList<Text>());
            PageWritable page = new PageWritable(1.0, new ArrayList<Text>());
            String[] outlinks = value.toString().split("\\s");
            for (String outlink : outlinks) {
                page.getOutlinks().add(new Text(outlink));
            }
            context.write(new Text(key), page);
        }
    }
}
