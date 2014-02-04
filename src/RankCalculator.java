import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 1:04 PM
 */
public class RankCalculator {

    private Job job = null;

    public Job getJob() {
        return job;
    }

    public RankCalculator(String input, String output) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        job = new Job(config, "RankCalculator");

        job.setJarByClass(RankCalculator.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RankCalcWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageWritable.class);

        job.setMapperClass(Map.class);
//        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

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

    private static class Map extends Mapper<Text, PageWritable, Text, RankCalcWritable> {

        @Override
        protected void map(Text key, PageWritable value, Context context) throws IOException, InterruptedException {
            double rank = value.getRank();
            ArrayList<Text> outLinks = value.getOutLinks();
            context.write(new Text("#sum"), new RankCalcWritable(rank));
            if (outLinks.size() > 0) {
                rank = rank / outLinks.size();
                for (Text outLink : outLinks) {
                    context.write(outLink, new RankCalcWritable(rank));
                }
            } else {    // sink rank
                context.write(new Text("#sink"), new RankCalcWritable(rank));
            }
            context.write(key, new RankCalcWritable(value));
        }
    }

    private static class Reduce extends Reducer<Text, RankCalcWritable, Text, PageWritable> {

        @Override
        protected void reduce(Text key, Iterable<RankCalcWritable> values, Context context) throws IOException, InterruptedException {

            double rank = 0;
            ArrayList<Text> outLinks = null;
            for (RankCalcWritable value : values) {
                if (value.isRankOrPage) {
                    rank += value.rank;
                } else {
                    outLinks = value.page.getOutLinks();
                }
            }
            String key_str = key.toString();
            if (key_str.equals("#sink") || key_str.equals("#sum")) {
                FSDataOutputStream outputStream = FileSystem.get(context.getConfiguration()).create(new Path("tmp/" + key_str.substring(1)));
                outputStream.writeDouble(rank);
                outputStream.close();
                System.out.println(rank + key_str);
            } else {
                if (rank != 0 || outLinks != null) {
                    context.write(key, new PageWritable(rank, outLinks));
                } else {
                    throw new NullPointerException("intermediate pairs missing");
                }
            }
        }
    }

//    // TODO fix
//    private static class Combine extends Reducer<Text, RankCalcWritable, Text, RankCalcWritable> {
//        @Override
//        protected void reduce(Text key, Iterable<RankCalcWritable> values, Context context) throws IOException, InterruptedException {
//            double rank = 0;
//            for (RankCalcWritable value : values) {
//                if (value.isRankOrPage) {
//                    rank += value.rank;
//                } else {
//                    context.write(key, new RankCalcWritable(value.outlinks));
//                }
//            }
//            context.write(key, new RankCalcWritable(rank));
//        }
//    }

    // there are no objects in static class thus the single class be reused without constructing new instances
    private static class RankCalcWritable implements Writable {    // used as value class of Mapper output and Reducer input

        private PageWritable page = null;

        private boolean isRankOrPage;
        private double rank; // calculated pagerank value from a particular inlink

        public RankCalcWritable() {
        }

        // TODO remove constructor from static class
        public RankCalcWritable(double rank) {   // for writing results as Mapper output
            this.rank = rank;
            isRankOrPage = true;
        }

        public RankCalcWritable(PageWritable page) {
            this.page = page;
            isRankOrPage = false;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeBoolean(isRankOrPage);
            if (isRankOrPage) {
                dataOutput.writeDouble(rank);
            } else {
                page.write(dataOutput);
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            isRankOrPage = dataInput.readBoolean();
            if (isRankOrPage) {
                rank = dataInput.readDouble();
                page = null;
            } else {
                rank = 0;
                page = new PageWritable();
                page.readFields(dataInput);
            }
        }
    }
}