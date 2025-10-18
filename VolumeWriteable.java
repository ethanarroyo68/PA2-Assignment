package ngram;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import java.util.Map;

//output value class
public class VolumeWriteable implements Writable {
	private IntWritable count;
	private MapWritable volumeIds;

	public VolumeWriteable(MapWritable volumeIds, IntWritable count) {
		// #TODO#: Initialize class variables using the constructor parameters
		this.count = count;
		this.volumeIds = volumeIds;

	}

	public VolumeWriteable() {
		// #TODO#: Initialize class variables with appropriate default values
		count = new IntWritable(0);
		volumeIds = new MapWritable();

	}

	public IntWritable getCount() {
		return count;
	}

	public MapWritable getVolumeIds() {
		return volumeIds;
	}

	public void set(MapWritable volumeIds, IntWritable count) {
		// #TODO#: Update class variables with the provided values
		this.count = count;
		this.volumeIds = volumeIds;
	}

	public void insertMapValue(Text key, IntWritable value) {
		volumeIds.put(key, value);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		count.readFields(arg0);
		volumeIds.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		count.write(arg0);
		volumeIds.write(arg0);
	}

	@Override
	public String toString() {
		return count.get() + "\t" + volumeIds.size();
	}

	public String unigramString() {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<Writable, Writable> entry : volumeIds.entrySet()) {
			sb.append(entry.getKey().toString())
					.append("\t")
					.append(entry.getValue().toString())
					.append("\n");
		}
		return sb.toString().trim();
	}

	@Override
	public int hashCode() {
		return volumeIds.hashCode();
	}
}