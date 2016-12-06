import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;


public class Question_3 {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String input = otherArgs[0];
        String output = otherArgs[1];
        String output_temp = otherArgs[1] + "tmp";
        int maximum = Integer.parseInt(otherArgs[2]);

        Job job1 = Job.getInstance(conf, "Question_2_1");
        Job job2 = Job.getInstance(conf, "Question_2_1");

        job1.setJarByClass(Question_2_1.class);
        job2.setJarByClass(Question_2_1.class);

        job2.getConfiguration().setInt("maximum", maximum);

        /* Set Inputs and Outputs */
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        /* Set Mapper, Reducer and Combiner for job1 */
        job1.setMapperClass(Job1Mapper.class);
        job1.setMapOutputKeyClass(CountryAndTagWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setReducerClass(Job1Reducer.class);
        job1.setOutputKeyClass(CountryAndTagWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setCombinerClass(Job1Reducer.class);
        job1.setNumReduceTasks(3);

        /* Set Mapper, Reducer and Combiner for job2 */
        job2.setMapperClass(Job2Mapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(TagAndOccurences.class);

        job2.setReducerClass(Job2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setCombinerClass(Job2Combiner.class);
        job2.setNumReduceTasks(3);

        /* Set files */
        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output_temp));
        FileInputFormat.addInputPath(job2, new Path(output_temp));
        FileOutputFormat.setOutputPath(job2, new Path(output));

        /* start job1 */
        job1.waitForCompletion(true);
        /* continue with job2 */
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

    public static class Job1Mapper extends Mapper<LongWritable, Text, CountryAndTagWritable, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            String tags = java.net.URLDecoder.decode(parts[8], "UTF-8");
            //String tag = parts[8];
            String lon = parts[10];
            String lat = parts[11];
            Country country = Country.getCountryAt(Double.parseDouble(lat), Double.parseDouble(lon));
            if (country != null) {
                String[] tags_split = tags.split(",");
                CountryAndTagWritable sandi = null;
                for (String tag : tags_split) {
                    if (!tag.isEmpty()) {
                        sandi = new CountryAndTagWritable();
                        sandi.set(country.toString(), tag);
                        context.write(sandi, new IntWritable(1));
                    }
                }
            }
        }
    }

    public static class Job1Reducer extends Reducer<CountryAndTagWritable, IntWritable, CountryAndTagWritable, IntWritable> {
        @Override
        protected void reduce(CountryAndTagWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class Job2Mapper extends Mapper<CountryAndTagWritable, IntWritable, Text, TagAndOccurences> {

        @Override
        protected void map(CountryAndTagWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            Text newKey = key.country;
            TagAndOccurences newValue = new TagAndOccurences();
            newValue.set(key.tag.toString(), value.get());
            context.write(newKey, newValue);
        }
    }

    public static class Job2Reducer extends Reducer<Text, TagAndOccurences, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<TagAndOccurences> values, Context context) throws IOException, InterruptedException {
            MinMaxPriorityQueue<TagAndOccurences> priolist = MinMaxPriorityQueue
                    .maximumSize(context.getConfiguration().getInt("maximum", 5))
                    .create();
            TagAndOccurences current = null;
            for (TagAndOccurences val : values) {
                current = new TagAndOccurences();
                current.set(val.tag.toString(), val.occurrences.get());
                priolist.add(current);
            }
            Iterator it = priolist.iterator();
            while (it.hasNext()) {
                current = (TagAndOccurences) it.next();
                context.write(key, new Text(current.tag.toString() + " " + current.occurrences.get()));
                it.remove();
            }
        }
    }

    public static class Job2Combiner extends Reducer<Text, TagAndOccurences, Text, TagAndOccurences> {
        @Override
        protected void reduce(Text key, Iterable<TagAndOccurences> values, Context context) throws IOException, InterruptedException {
            MinMaxPriorityQueue<TagAndOccurences> priolist = MinMaxPriorityQueue
                    .maximumSize(context.getConfiguration().getInt("maximum", 5))
                    .create();
            TagAndOccurences current = null;
            for (TagAndOccurences val : values) {
                current = new TagAndOccurences();
                current.set(val.tag.toString(), val.occurrences.get());
                priolist.add(current);
            }
            Iterator it = priolist.iterator();
            while (it.hasNext()) {
                current = (TagAndOccurences) it.next();
                context.write(key, current);
                it.remove();
            }
        }
    }

}
