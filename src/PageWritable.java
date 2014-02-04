/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 2:50 PM
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class PageWritable implements Writable {   // used as value class of Mapper input and Reducer output

    private double rank = 0;
    private ArrayList<Text> outLinks = null;

    public  PageWritable() {
        outLinks = new ArrayList<Text>();
    }

    public PageWritable(double rank, ArrayList<Text> outLinks) {
        this.rank = rank;
        this.outLinks = outLinks;
    }

    @Override
    public String toString() {
        return "PageWritable{" +
                "rank=" + rank +
                ", outLinks=" + outLinks +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(rank);
        dataOutput.writeInt(outLinks.size());
        for(Text outLink: outLinks) {
            outLink.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rank = dataInput.readDouble();
        int nOutLinks = dataInput.readInt();
        while (nOutLinks-->0) {
            Text outLink = new Text();
            outLink.readFields(dataInput);
            outLinks.add(outLink);
        }
//        System.out.println("PWr#"+this);
    }

    public double getRank() {
        return rank;
    }

    public ArrayList<Text> getOutLinks() {
        return outLinks;
    }
}