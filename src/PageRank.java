import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

/**
 * Created with IntelliJ IDEA.
 * User: FateAKong
 * Date: 10/20/13
 * Time: 8:42 PM
 */
public class PageRank {
    public static void main(String[] args) throws Exception {
        JobControl control = new JobControl("PageRank");
        JobController controller = new JobController(control);

        InLinkGraphGenerator in = new InLinkGraphGenerator("input", "output");
        ControlledJob inJob = new ControlledJob(in.getConfig());
        control.addJob(inJob);
        Thread t = new Thread(controller);
        t.start();
        while (!control.allFinished()) {
            System.out.println("running ");
            Thread.sleep(5000);
        }
        System.out.println("end");

//        GraphPropertyAnalyzer fp = new GraphPropertyAnalyzer(args[0], args[1]+"/0");
//        ControlledJob fpjob = new ControlledJob(fp.getConfig());
//        int iIter = 0;
//        do {
//            System.out.println("begin iteration #"+iIter);
//            RankCalculator rc = new RankCalculator(args[1]+"/"+Integer.toString(iIter), args[1]+"/"+Integer.toString(iIter+1));
//            ControlledJob rcjob = new ControlledJob(rc.getConfig());
//            isConverged = true;
//            if (iIter==0) {
//                rcjob.addDependingJob(fpjob);
//                control.addJob(rcjob);
//                control.addJob(fpjob);
//            } else {
//                control.addJob(rcjob);
//            }
//            Thread t = new Thread(controller);
//            t.start();
//            while (!control.allFinished()) {
//                System.out.println("running iteration #"+iIter);
//                Thread.sleep(5000);
//            }
//            System.out.println("end iteration #"+iIter);
//            iIter++;
//        } while (!isConverged);
//
//        RankSorter rs = new RankSorter(args[1]+"/"+Integer.toString(iIter), args[2]);
//        ControlledJob rsjob = new ControlledJob(rs.getConfig());
//        control.addJob(rsjob);
//        Thread t = new Thread(controller);
//        t.start();
//        while (!control.allFinished()) {
//            System.out.println("sorting");
//            Thread.sleep(5000);
//        }
//        System.out.println("num of iterations: "+iIter);
        System.exit(0);
    }
}