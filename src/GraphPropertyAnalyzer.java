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
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 4:08 PM
 */
public class GraphPropertyAnalyzer {

    private Job job = null;

    public Configuration getConfig() {
        return job.getConfiguration();
    }

    public GraphPropertyAnalyzer(String input, String output) throws IOException {
        Configuration config = new Configuration();
        config.set("xmlinput.start", "<page>");
        config.set("xmlinput.end", "</page>");

        job = new Job(config, "GraphPropertyAnalyzer");

        job.setJarByClass(GraphPropertyAnalyzer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(XMLInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);

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
                while (reader.hasNext()) {
                    int eventCode = reader.next();
                    switch (eventCode) {
                        case XMLStreamConstants.CHARACTERS:
                            content = reader.getText().trim();
                            break;
                        case XMLStreamConstants.END_ELEMENT:
                            String end = reader.getLocalName();
                            if ("title".equals(end)) {
                                context.write(new Text("counter"), new Text(content.replace(' ', '_')));
                            } else if ("text".equals(end)) {
                                int b, e = 0;
                                while ((b = content.indexOf("[[", e))!=-1) {
                                    e = content.indexOf("]]", b);
                                    String wikiLink = content.substring(b+2, e);
                                    if (wikiLink.contains(":")) continue;
                                    int barPos;
                                    if ((barPos = wikiLink.indexOf('|'))!=-1) {
                                        wikiLink = wikiLink.substring(0, barPos);
                                    }
                                    context.write(new Text("counter"), new Text(wikiLink.replace(' ', '_')));
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
            if (key.toString().equals("counter")) {
                HashSet<Text> outLinks = new HashSet<Text>();
                for (Text value: values) {
                    if (!outLinks.contains(value)) {
                        outLinks.add(new Text(value));
                    }
                }
                context.write(new Text("N=" + String.valueOf(outLinks.size())), new Text());
            }
        }
    }
}
