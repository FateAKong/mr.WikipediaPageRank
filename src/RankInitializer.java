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

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 4:08 PM
 */
public class RankInitializer {

    private Job job = null;

    public Job getJob() {
        return job;
    }

    public RankInitializer(String input, String output) throws IOException {
        Configuration config = new Configuration();
        config.set("xmlinput.start", "<page>");
        config.set("xmlinput.end", "</page>");

        job = new Job(config, "RankInitializer");

        job.setJarByClass(RankInitializer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageWritable.class);

        job.setMapperClass(Map.class);
//        job.setNumReduceTasks(0);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(XMLInputFormat.class);
        job.setOutputFormatClass(PageOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        Path outputPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outputPath);
        FileSystem fs = FileSystem.get(outputPath.toUri(), config);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
    }

    private static class Map extends Mapper<LongWritable, Text, Text, PageWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            XMLInputFactory factory = XMLInputFactory.newFactory();
            String title = null;
            ArrayList<Text> outLinks = new ArrayList<Text>();
            PageWritable page = new PageWritable(1, outLinks);
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
                                title = content.replace(' ', '_');
                                if (title.contains("/")) return;
                            } else if ("text".equals(end)) {
                                int b, e = 0;
                                while ((b = content.indexOf("[[", e))!=-1) {
                                    e = content.indexOf("]]", b);
                                    String wikiLink = content.substring(b+2, e);
                                    if (wikiLink.contains(":")||wikiLink.contains("#")||wikiLink.contains("/")||wikiLink.equals(title)) continue;
                                    int barPos;
                                    if ((barPos = wikiLink.indexOf('|'))!=-1) {
                                        wikiLink = wikiLink.substring(0, barPos);
                                    }
                                    wikiLink = wikiLink.replace(' ', '_');
                                    outLinks.add(new Text(wikiLink));
                                    // also emit a PageWritable with empty outLinks list for each item in outLinks
                                    context.write(new Text(wikiLink), new PageWritable(1.0, new ArrayList<Text>()));
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
            context.write(new Text(title), page);
        }
    }

    // Reducer is simply used to handle pages only appear in texts (otherwise only Mapper is fine)
    private static class Reduce extends Reducer<Text, PageWritable, Text, PageWritable> {
        @Override
        protected void reduce(Text key, Iterable<PageWritable> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
            ArrayList<Text> outLinks = new ArrayList<Text>();
            for (PageWritable value: values) {
                for (Text outLink: value.getOutLinks()) {
                    outLinks.add(new Text(outLink));
                }
            }
            context.write(key, new PageWritable(1, outLinks));
        }
    }
}
