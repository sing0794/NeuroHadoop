package convolution.rchannel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * ConvolutionJob
 * 
 * This is the main job class of the Map Reduce job.
 * 
 * From here we wire together the Map, Reduce, Partition, and Writable classes.
 * 
 */

public class ConvolutionJob extends Configured implements Tool {

    public static final String LOCAL_KERNEL = "/home/ashish/data/morlet-2000.csv";
    public static final String LOCAL_CHANNEL = "/home/ashish/data/R187-2009-11-08-CSC6a.csv";

    public static final String HDFS_KERNEL = "lookup/morlet-2000.dat";
    public static final String HDFS_CHANNEL = "input/R187-2009-11-08-CSC6a.dat";

    public void cacheKernel(JobConf conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path hdfsPath = new Path(HDFS_KERNEL);

        // upload kernel file to hdfs. Overwrite any existing copy.
        fs.copyFromLocalFile(false, true, new Path(LOCAL_KERNEL), hdfsPath);

        DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
    }

    public void hdfsSetup(JobConf conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path hdfsPath = new Path(HDFS_CHANNEL);

        // upload input channel file to hdfs. Overwrite any existing copy.
        fs.copyFromLocalFile(false, true, new Path(LOCAL_CHANNEL), hdfsPath);

        this.cacheKernel(conf);
    }

	@Override
	public int run(String[] args) throws Exception {

		System.out.println("\n\nConvolutionJob\n");

		JobConf conf = new JobConf(getConf(), ConvolutionJob.class);
		conf.setJobName("ConvolutionJob");

        	this.hdfsSetup(conf);

		conf.setMapOutputKeyClass(TimeseriesKey.class);
		conf.setMapOutputValueClass(TimeseriesDataPoint.class);

		conf.setMapperClass(ConvolutionMapper.class);
		conf.setReducerClass(ConvolutionReducer.class);

		conf.setPartitionerClass(NaturalKeyPartitioner.class);
		conf.setOutputKeyComparatorClass(CompositeKeyComparator.class);
		conf.setOutputValueGroupingComparator(NaturalKeyGroupingComparator.class);


        List<String> other_args = new ArrayList<String>();
        for(int i=0; i < args.length; ++i) {
         try {
           if ("-m".equals(args[i])) {
           	
             conf.setNumMapTasks(Integer.parseInt(args[++i]));
             
           } else if ("-r".equals(args[i])) {
           	
             conf.setNumReduceTasks(Integer.parseInt(args[++i]));
	           		    	   
           } else {
           	
             other_args.add(args[i]);
             
           }
         } catch (NumberFormatException except) {
           System.out.println("ERROR: Integer expected instead of " + args[i]);
           return printUsage();
         } catch (ArrayIndexOutOfBoundsException except) {
           System.out.println("ERROR: Required parameter missing from " +
                              args[i-1]);
           return printUsage();
         }
        }
        // Make sure there are exactly 2 parameters left.
        if (other_args.size() != 2) {
         System.out.println("ERROR: Wrong number of parameters: " +
                            other_args.size() + " instead of 2.");
         return printUsage();
        }
	
		
		conf.setInputFormat(TextInputFormat.class);

		conf.setOutputFormat(TextOutputFormat.class);
		conf.setCompressMapOutput(true);

		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);

		return 0;
	}

	static int printUsage() {
		System.out.println("ConvolutionJob [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new ConvolutionJob(), args);
		System.exit(res);

	}

}
