import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TagAndOccurences implements Comparable<TagAndOccurences>, Writable {
	public Text tag;
	public IntWritable occurrences;

	public TagAndOccurences(){
		this.tag = new Text();
		this.occurrences = new IntWritable();
	}
	public TagAndOccurences(String tag, int occurrences){
		this();
		this.tag = new Text();
		this.occurrences = new IntWritable();
	}

	public void set(String string, int integer){
		this.tag.set(string);
		this.occurrences.set(integer);
	}
	@Override
	public int compareTo(TagAndOccurences o) {
		return Integer.compare(o.occurrences.get(), occurrences.get());
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.tag.set(arg0.readUTF());
		this.occurrences.set(arg0.readInt());
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(tag.toString());
		arg0.writeInt(occurrences.get());
	}

}
