
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.MinMaxPriorityQueue;


public class Question_2_2 {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, TagAndOccurences> {
		  
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] parts = line.split("\t");
			String tags = java.net.URLDecoder.decode(parts[8], "UTF-8");
			//String tag = parts[8];
			String lon = parts[10];
			String lat = parts[11];
			Country country = Country.getCountryAt(Double.parseDouble(lat),Double.parseDouble(lon));
			if ( country != null){
				Text textcountry = new Text(country.toString());
				String[] tags_split = tags.split(",");
				TagAndOccurences sandi = null;
				for (String tag : tags_split){
					sandi = new TagAndOccurences();
					sandi.set(tag, 1);
					context.write(textcountry, sandi);
				}
				//context.write(text, new IntWritable(1));
				//context.write(value, new IntWritable(1));
			}
		}
	}

	public static class MyReducer extends Reducer<Text, TagAndOccurences, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<TagAndOccurences> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> tags_counter = new HashMap<String,Integer>();
			int lol = 0 ;
			TagAndOccurences current = null;
			
			for (TagAndOccurences val : values) {
				if (tags_counter.containsKey(val.tag.toString())){
					lol = tags_counter.get(val.tag.toString());
					tags_counter.put(val.tag.toString(), lol+1);
					//context.write(key, new Text( val.tag.toString() + " -> " + lol));
				}else{
					tags_counter.put(val.tag.toString(), val.occurrences.get());
					//context.write(key, new Text( val.tag.toString() + " - " + val.occurrences.get()));
				}
			}

		    MinMaxPriorityQueue<TagAndOccurences> priolist = MinMaxPriorityQueue.create();
		    
			Iterator it = tags_counter.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        current = new TagAndOccurences();
		        current.set((String)pair.getKey(), (Integer)pair.getValue());
		        priolist.add(current);
				//context.write(key, new Text((String)pair.getKey() + "- "+ (Integer)pair.getValue()));
		        it.remove(); // avoids a ConcurrentModificationException
		    }
		    
		    int index = 0;
		    //while(!priolist.isEmpty() && index < 1000){
			while(!priolist.isEmpty() && index < context.getConfiguration().getInt("maximum", 5)){
		    	current = priolist.pollLast();
				context.write(key, new Text(current.tag.toString() + " " + current.occurrences.get()));
		    	index++;
		    }
		    
			
		}
	}
	
	public static class MyCombiner extends Reducer<Text, TagAndOccurences, Text, TagAndOccurences> {
		@Override
		protected void reduce(Text key, Iterable<TagAndOccurences> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> tags_counter = new HashMap<String,Integer>();
			int lol = 0 ;
			TagAndOccurences current = null;
			
			for (TagAndOccurences val : values) {
				if (tags_counter.containsKey(val.tag.toString())){
					lol = tags_counter.get(val.tag.toString());
					tags_counter.put(val.tag.toString(), lol+1);
					//context.write(key, new Text( val.tag.toString() + " -> " + lol));
				}else{
					tags_counter.put(val.tag.toString(), val.occurrences.get());
					//context.write(key, new Text( val.tag.toString() + " - " + val.occurrences.get()));
				}
			}
			Iterator it = tags_counter.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        current = new TagAndOccurences();
		        current.set((String)pair.getKey(), (Integer)pair.getValue());
				context.write(key, current);
		        it.remove(); // avoids a ConcurrentModificationException
		    }
		    
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		int maximum = Integer.parseInt(otherArgs[2]);
		
		Job job = Job.getInstance(conf, "Question_2_1");
		job.setJarByClass(Question_2_1.class);
		
		job.getConfiguration().setInt("maximum", maximum);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TagAndOccurences.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setCombinerClass(MyCombiner.class);
		job.setNumReduceTasks(3);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
