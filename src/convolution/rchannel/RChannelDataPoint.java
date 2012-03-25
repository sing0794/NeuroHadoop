package convolution.rchannel;

//import java.text.ParseException;
//import java.text.SimpleDateFormat;

/**
 * RChannel DataPoint
 * 
 * Primary function is to parse and hold the data from a line of RChannel CSV
 * data.
 * 
 * It is the main "glue code" one would need to provide to make their timeseries
 * data source work with this example.
 * 
 */
public class RChannelDataPoint {

	public String timestamp;
	public String voltage = String.valueOf(0);

	public short getVoltage() {
		return Short.parseShort(this.voltage);

	}

	public long getTimestamp() {
		return Long.parseLong(this.timestamp);

	}

	public static RChannelDataPoint parse(String csvRow) {

		RChannelDataPoint rec = new RChannelDataPoint();

		String[] values = csvRow.split(",");

		if (values.length != 3) {
			return null;
		}

		rec.timestamp = values[0].trim();
		rec.voltage = values[1].trim();

		return rec;

	}

}
