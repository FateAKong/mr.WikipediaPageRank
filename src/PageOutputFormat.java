import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 6:06 PM
 */
public class PageOutputFormat extends FileOutputFormat<Text, PageWritable> {

    @Override
    public RecordWriter<Text, PageWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Path file = getDefaultWorkFile(taskAttemptContext, "");
        FileSystem fs = file.getFileSystem(taskAttemptContext.getConfiguration());
        FSDataOutputStream dos = fs.create(file, taskAttemptContext);
        return new PageRecordWriter(dos);
    }

    private static class PageRecordWriter extends RecordWriter<Text, PageWritable> {

        private DataOutputStream dos;

        public PageRecordWriter(DataOutputStream dos) {
            this.dos = dos;
        }

        @Override
        public void write(Text text, PageWritable pageWritable) throws IOException, InterruptedException {
            if (text == null || pageWritable == null) return;
            String line = text.toString() + '\t' + String.valueOf(pageWritable.getRank());
            ArrayList<Text> outLinks = pageWritable.getOutlinks();
            if (outLinks != null) { // sinks have no outLinks but only rank value
                for (Text outLink : outLinks) {
                    line += ' ' + outLink.toString();
                }
            }
            line += '\n';
            dos.write(line.getBytes("UTF-8"));
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            dos.close();
        }
    }
}
