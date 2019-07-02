package reducesort;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class Sort extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("Sort Job");
        job.setJar("GetTop1000AndSort.jar");
        job.setJarByClass(Sort.class);

        job.setMapperClass(Sort.MapSortTop1000.class);
        job.setReducerClass(Sort.RedSortTop1000.class);

        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(strings[1]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        return job.waitForCompletion(true)? 0: 1;
    }

    public static class MapSortTop1000 extends Mapper<Object, Text, LongWritable, Text> {

        private final String separator = "\t";
        private Text tag = new Text();
        private LongWritable count = new LongWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(separator);
            tag.set(parts[0] + separator + parts[1]);
            count.set(Long.parseLong(parts[2].trim()));
            context.write(count, tag);
        }
    }

    public static class RedSortTop1000 extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value: values) {
                context.write(key, value);
            }
        }
    }
}
