import edu.umd.cloud9.collection.XMLInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
 * Parse Wikipedia XML data (essentially an unformatted outlink graph) into inlink graph
 */
public class InlinkGenerator {

    private Job job = null;

    public Job getJob() {
        return job;
    }

    public InlinkGenerator(String input, String output) throws IOException {
        Configuration config = new Configuration();
        config.set("xmlinput.start", "<page>");
        config.set("xmlinput.end", "</page>");

        job = new Job(config, "InlinkGenerator");

        job.setJarByClass(InlinkGenerator.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(XMLInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        Path outputPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outputPath);
        FileSystem fs = FileSystem.get(outputPath.toUri(), config);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
    }

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            XMLInputFactory factory = XMLInputFactory.newFactory();
            try {
                XMLStreamReader reader = factory.createXMLStreamReader(new StringReader(value.toString()));
                String content = null;
                String title = null;
                while (reader.hasNext()) {
                    int eventCode = reader.next();
                    switch (eventCode) {
                        case XMLStreamConstants.CHARACTERS:
                            content = reader.getText().trim();
                            break;
                        case XMLStreamConstants.END_ELEMENT:
                            String end = reader.getLocalName();
                            if (end.equals("title")) {
                                title = content.replace(' ', '_');
                                // remove subpage titles
                                if (title.contains("/")) return;
                                // attach a #non-red inlink to each <title> page to indicate in reducer part
                                context.write(new Text(title), new Text("#non-red"));
                            } else if (end.equals("text")) {
                                int b, e = 0;
                                boolean isSink = true;
                                while ((b = content.indexOf("[[", e)) != -1) {
                                    e = content.indexOf("]]", b);
                                    String outlink = content.substring(b + 2, e);
                                    if (outlink.contains(":") || outlink.contains("#") || outlink.contains("/") || outlink.equals(title))
                                        continue;
                                    int barPos;
                                    if ((barPos = outlink.indexOf('|')) != -1) {
                                        outlink = outlink.substring(0, barPos);
                                    }
                                    isSink = false;
                                    context.write(new Text(outlink.replace(' ', '_')), new Text(title));
                                }
                                // create a dummy outlink page #sink for pages with no outlinks (i.e. sinks/dead-ends)
                                // in order to preserve all the nodes when generating inlink graph
                                if (isSink) {
                                    context.write(new Text("#sink"), new Text(title));
                                }
                            }
                            break;
                        default:
                            break;
                    }
                }
            } catch (XMLStreamException e) {
                e.printStackTrace();
            }

        }
    }

    private static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashSet<Text> inlinks = new HashSet<Text>();
            StringBuilder output = null;
            boolean toKeep = key.toString().equals("#sink");   // the <title> pages and the fake #sink page should be preserved
            for (Text value : values) {
                String value_str = value.toString();
                if (value_str.equals("#non-red")) {
                    toKeep = true;
                } else {
                    if (!inlinks.contains(value)) {
                        if (output == null) {
                            output = new StringBuilder(value_str);
                        } else {
                            output.append(' ');
                            output.append(value_str);
                        }
                        inlinks.add(new Text(value_str));
                    }
                }
            }

            if (key.toString().equals("#sink")) System.out.println("sink has values: " + output.toString());

            // output could be null when the page has no real inlinks (there's only a #non-red flag as inlink)
            // in this case just skip this page since it won't get lost during outlink->inlink graph transformation
            if (toKeep && output!=null) {
                context.write(key, new Text(output.toString()));
            }
        }
    }
}
