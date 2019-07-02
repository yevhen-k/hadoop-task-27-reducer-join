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


public class MapRedTop1000 extends Configured implements Tool {

    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJar("GetTop1000AndSort.jar");
        job.setJarByClass(MapRedTop1000.class);
        job.setJobName("MapRed Job");

        job.setMapperClass(MapRedTop1000.TopMapper.class);
        job.setReducerClass(MapRedTop1000.TopReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true)? 0: 1;
    }

    public static class TopMapper extends Mapper<Object, Text, Text, LongWritable> {

        private final String separator = "\t";
        private Text tag = new Text();
        private LongWritable count = new LongWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(separator);
            tag.set(parts[1].trim() + separator + parts[2].trim());
            count.set(Long.parseLong(parts[3].trim()));
            context.write(tag, count);
        }
    }

    public static class TopReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value: values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
}
