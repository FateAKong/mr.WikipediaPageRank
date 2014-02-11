import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Parse outlink graph into equivalent inlink graph
 */
public class OutlinkGenerator {

    private static Job job = null;
    private static int nPages;

    public Job getJob() {
        return job;
    }

    public OutlinkGenerator(String input, String output) throws IOException {
        job = new Job(new Configuration(), "OutlinkGenerator");

        job.setJarByClass(OutlinkGenerator.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
    }

    private static class Map extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] outlinks = value.toString().split("\\s");
            for (String outlink : outlinks) {
                if (outlink.equals("#non-red")) {
                    context.write(key, new Text("#non-red"));
                } else {
                    context.write(new Text(outlink), key);
                }
            }
        }
    }

    private static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder output = null;
            for (Text value : values) {
                String value_str = value.toString();
                if (!value_str.equals("#non-red")) {
                    if (output==null) {
                        output = new StringBuilder(value_str);
                    } else {
                        output.append(' ');
                        output.append(value_str);
                    }
                }
            }
            context.write(key, new Text(output==null? "" : output.toString()));
        }
    }
}
