import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CountryAndTagWritable implements WritableComparable<CountryAndTagWritable>, Writable {
	public Text country;
	public Text tag;

	public CountryAndTagWritable(){
		this.country = new Text();
		this.tag = new Text();
	}
	public CountryAndTagWritable(String country, int tag){
		this();
		this.country = new Text();
		this.tag = new Text();
	}
	
	public void set(String country, String tag){
		this.country.set(country);
		this.tag.set(tag);
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.country.set(arg0.readUTF());
		this.tag.set(arg0.readUTF());
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(country.toString());
		arg0.writeUTF(tag.toString());
	}
	@Override
	public int compareTo(CountryAndTagWritable o) {
		return this.toString().compareTo(o.toString());
	}
	@Override
	public String toString() {
		return country + " " + tag;
	}

}
