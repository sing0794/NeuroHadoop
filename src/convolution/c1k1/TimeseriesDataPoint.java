package convolution.c1k1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TimeseriesDataPoint implements Writable,
		Comparable<TimeseriesDataPoint> {

	public long lDateTime;
	public long fValue;

	/**
	 * Deserializes the point from the underlying data.
	 * 
	 * @param in
	 *            A DataInput object to read the point from.
	 * @see java.io.DataInput
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 * 
	 */
	public void readFields(DataInput in) throws IOException {

		this.lDateTime = in.readLong();
		this.fValue = in.readLong();
	}

	/**
	 * This is a static method that deserializes a point from the underlying
	 * binary representation.
	 * 
	 * @param in
	 *            A DataInput object that represents the underlying stream to
	 *            read from.
	 * @return A TimeseriesDataPoint
	 * @throws IOException
	 */
	public static TimeseriesDataPoint read(DataInput in) throws IOException {

		TimeseriesDataPoint p = new TimeseriesDataPoint();
		p.readFields(in);
		return p;

	}

	public void copy(TimeseriesDataPoint source) {

		this.lDateTime = source.lDateTime;
		this.fValue = source.fValue;

	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeLong(this.lDateTime);
		out.writeLong(this.fValue);

	}

	/**
	 * This is only used in the case of manually sorting the data in the reducer
	 * 
	 * Map Reduce itself does not use this method for sorting the data.
	 * 
	 */
	@Override
	public int compareTo(TimeseriesDataPoint oOther) {
		if (this.lDateTime < oOther.lDateTime) {
			return -1;
		} else if (this.lDateTime > oOther.lDateTime) {
			return 1;
		}

		// default -- they are equal
		return 0;
	}

}
