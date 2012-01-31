package convolution.rchannel;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ConvolutionReducer extends MapReduceBase implements
		Reducer<TimeseriesKey, TimeseriesDataPoint, Text, Text> {

    public static final String HDFS_KERNEL = "lookup/morlet-2000.dat";

	static enum PointCounters {
		POINTS_SEEN, POINTS_ADDED_TO_WINDOWS, MOVING_AVERAGES_CALCD
	};

    private HashMap<Integer, String> kernelMap;

	private JobConf configuration;

	@Override
	public void configure(JobConf conf) {
		this.configuration = conf;

        try {
            String kernelCacheName = new Path(HDFS_KERNEL).getName();
            Path [] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
            if (null != cacheFiles && cacheFiles.length > 0) {
                for (Path cachePath : cacheFiles) {
                    if (cachePath.getName().equals(kernelCacheName)) {
                        loadKernel(cachePath);
                        break;
                    }
                }
            }
        } catch (IOException ioe) {
            System.err.println("IOException reading from distributed cache");
            System.err.println(ioe.toString());
        }
    }

    public void loadKernel(Path cachePath) throws IOException {
        BufferedReader kernelReader = new BufferedReader(new FileReader(cachePath.toString()));
        try {
            String line = "";
            int kernelFreq = 5;
            this.kernelMap = new HashMap<Integer, String>();
            while ((line = kernelReader.readLine()) != null) {
                this.kernelMap.put(kernelFreq, line);
                kernelFreq++;
            }
        } finally {
            kernelReader.close();
        }
    }

    public Long[] ConvertStringArrayToLongArray(String[] stringArray){
        Long longArray[] = new Long[stringArray.length];

        for(int i = 0; i < stringArray.length; i++){
            longArray[i] = Long.parseLong(stringArray[i]);
        }

        return longArray;
    }

	public void reduce(TimeseriesKey key, Iterator<TimeseriesDataPoint> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		long ckConvolution = 0;

		Long[] kernelStack = ConvertStringArrayToLongArray(kernelMap.get(100).split(","));
		
		int windowSize = kernelStack.length;

		Text out_key = new Text();
		out_key.set("");
		Text out_val = new Text();

		CircularArrayList sliding_window = new CircularArrayList(windowSize);	
		
		while (values.hasNext()) {

			while (sliding_window.size() < sliding_window.capacity() && values.hasNext()) {

				// reporter.incrCounter(PointCounters.POINTS_ADDED_TO_WINDOWS, 1);

				try {
					sliding_window.add(sliding_window.size(), values.next().fValue);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}

			if (sliding_window.size() == sliding_window.capacity()) {

				// reporter.incrCounter(PointCounters.MOVING_AVERAGES_CALCD, 1);

				ckConvolution = 0;

				for (int i = 0; i < sliding_window.size(); i++) {

					ckConvolution += sliding_window.get(i)*kernelStack[i];

				} // for

				out_val.set(String.valueOf(ckConvolution));

				output.collect(out_key, out_val);

				// 2. step window forward

				sliding_window.remove(0);

			}

		} // while

		out_key.set("debug > " + key.getGroup()
				+ " --------- end of group -------------");
		out_val.set("");

		output.collect(out_key, out_val);

	} // reduce

}
