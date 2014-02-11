import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * A final MapReduce procedure to sort and output rank results
 */
public class RankSorter {
    private static final double DAMPING_FACTOR = 0.85;
    private static double nPages;
    private static double prevSinkRank;
    private Job job = null;

    public Job getJob() {
        return job;
    }

    public RankSorter(String input, String output, int nPages) throws IOException, InterruptedException, ClassNotFoundException {
        RankSorter.nPages = nPages;
        Configuration config = new Configuration();
        job = new Job(config, "RankSorter");

        job.setJarByClass(RankFinalizer.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(PageInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setSortComparatorClass(DescendingDoubleComparator.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
    }

    private static class Map extends Mapper<Text, PageWritable, DoubleWritable, Text> {
        @Override
        protected void map(Text key, PageWritable value, Context context) throws IOException, InterruptedException {
            context.write(new DoubleWritable(value.getRank()), key);
        }
    }

    private static class Reduce extends Reducer<DoubleWritable, Text, Text, Text> {
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text k = new Text(key.toString());
            for (Text value: values) {
                context.write(new Text(value), k);
            }
        }
    }

    private static class DescendingDoubleComparator implements RawComparator<DoubleWritable> {

        private static final DoubleWritable.Comparator DOUBLE_COMPARATOR = new DoubleWritable.Comparator();
        @Override
        public int compare(byte[] bytes, int i, int i2, byte[] bytes2, int i3, int i4) {
            return (-1)*DOUBLE_COMPARATOR.compare(bytes, i, i2, bytes2, i3, i4);
        }

        @Override
        public int compare(DoubleWritable o1, DoubleWritable o2) {
            return (-1)*DOUBLE_COMPARATOR.compare(o1, o2);
        }

        @Override
        public boolean equals(Object obj) {
            return DOUBLE_COMPARATOR.equals(obj);
        }
    }
}