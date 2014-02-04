import edu.umd.cloud9.collection.XMLInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 4:08 PM
 */
public class PropertyAnalyzer {

    private Job job = null;

    public Job getJob() {
        return job;
    }

    public PropertyAnalyzer(String input, String output) throws IOException {

        job = new Job(new Configuration(), "PropertyAnalyzer");

        job.setJarByClass(PropertyAnalyzer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(PageInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(input));
        Path outputPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outputPath);
        FileSystem fs = FileSystem.get(outputPath.toUri(), job.getConfiguration());
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
    }

    private static class Map extends Mapper<Text, PageWritable, Text, NullWritable> {
        @Override
        protected void map(Text key, PageWritable value, Context context) throws IOException, InterruptedException {
            context.write(new Text("#counter"), NullWritable.get());
        }
    }

    private static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("#counter")) {
                int counter = 0;
                for (NullWritable value: values) {
                    counter++;
                }
                context.write(new Text("N=" + String.valueOf(counter)), NullWritable.get());
            }
        }
    }
}
