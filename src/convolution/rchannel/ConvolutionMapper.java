package convolution.rchannel;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * ConvolutionMapper
 * 
 * The primary purpose of this map class is to read each line from the CSV file
 * split, parse out the timeseries point record with the YahooStockDataPoint
 * class, and emit a timeseries k/v pair
 * 
 * In this case we're emitting custom key and value pairs for this example in
 * the TimeseriesKey ( key / WriteableComparable ) and TimeseriesDataPoint (
 * value / Writeable )
 * 
 */

public class ConvolutionMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

    public static final String HDFS_KERNEL = "lookup/morlet-2000.dat";
    public static final int SIGNAL_BUFFER_SIZE = 10000000;

	static enum Parse_Counters {
		BAD_PARSE
	};

	private JobConf configuration;
//	private final TimeseriesKey key = new TimeseriesKey();
	private final Text out_key = new Text();

	private final TimeseriesDataPoint val = new TimeseriesDataPoint();
    private HashMap<Integer, String> kernelMap;
    private short[] kernelStack;
    private int windowSize;
    
    private long ckConvolution = 0;
    private short[] signal = new short[SIGNAL_BUFFER_SIZE];
    private int n = 0;

    private RChannelDataPoint rec;
    
    private long lastTimestamp = 0;

	private static final Logger logger = Logger
			.getLogger(ConvolutionMapper.class);

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
                kernelStack = ConvertStringArrayToShortArray(kernelMap.get(100).split(","));
                windowSize = kernelStack.length;


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

    public short[] ConvertStringArrayToShortArray(String[] stringArray){
        short shortArray[] = new short[stringArray.length];

        for(int i = 0; i < stringArray.length; i++){
            shortArray[i] = Short.parseShort(stringArray[i]);
        }

        return shortArray;
    }
    
	public void map(LongWritable inkey, Text value,
        OutputCollector<Text, Text> output,
        Reporter reporter) throws IOException {

        rec = RChannelDataPoint.parse(value.toString());

        try {
            if (lastTimestamp > rec.getTimestamp()) {
                throw new IOException("Timestamp not sorted at: " + lastTimestamp + " and " + rec.getTimestamp());
            }
            
            lastTimestamp = rec.getTimestamp();
            
            if (n<SIGNAL_BUFFER_SIZE) {
                signal[n] = rec.getVoltage();

                if (n>=windowSize-1) {

                   ckConvolution = 0;

                   for (int i = n-windowSize+1, j=0; i <= n && j < windowSize; i++, j++) {
                       ckConvolution += signal[i]*kernelStack[j];
                   } // for

                out_key.set(String.valueOf(ckConvolution));
                output.collect(out_key, null);
                } // if

                n++;

            } else {
                n = 0;
                for (int j = SIGNAL_BUFFER_SIZE-windowSize+1; j<SIGNAL_BUFFER_SIZE; j++) {
                   signal[n] = signal[j];
                   n++;
                } //for
            } // if
            
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
            System.exit(0);
       }
	} // map

}
