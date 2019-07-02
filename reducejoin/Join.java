package reducejoin;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class Join extends Configured implements Tool {

    public static final String toJoin = "to_join";
    public static final String top = "top";
    public static final String delimiter = ":::::";

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("Reduce Side Join");
        job.setJar("Join.jar");
        job.setJarByClass(Join.class);

        MultipleInputs.addInputPath(job, new Path(strings[0]), TextInputFormat.class, Join.Top1000Mapper.class);
        MultipleInputs.addInputPath(job, new Path(strings[1]), TextInputFormat.class, Join.SecondDataPartMapper.class);
        job.setReducerClass(Join.ReduceSideJoiner.class);

        job.setNumReduceTasks(4);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        return job.waitForCompletion(true)? 0: 1;
    }

    /*
    * Read top1000.txt file
    * $ head top1000.txt
    * 470792377       Main_Page       external
    * 45314663        Hyphen-minus    external
    * 7615434 Hyphen-minus    other
    * 6190163 Captain_Marvel_(film)   external
    * 5960065 Luke_Perry      external
    * 4483625 Main_Page       other
    * 4118561 Us_(2019_film)  external
    * 3715584 Freddie_Mercury external
    * 2662482 Murder_of_Dee_Dee_Blanchard     external
    * 2596688 Disappearance_of_Madeleine_McCann       external
    * */
    public static class Top1000Mapper extends Mapper<Object, Text, Text, Text> {

        private final String separator = "\t";
        private Text key = new Text();
        private Text val = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(separator);
            this.key.set(parts[1] + separator + parts[2]);
            this.val.set(parts[0] + delimiter + top);
            context.write(this.key, this.val);
        }
    }


    /*
    * Read clickstream-enwiki-2019-04.tsv file
    * $ head clickstream-enwiki-2019-04.tsv
    * John_C._Reilly  Tim_and_Eric's_Billion_Dollar_Movie     link    17
    * Abso_Lutely_Productions Tim_and_Eric's_Billion_Dollar_Movie     link    90
    * Will_Ferrell_filmography        Tim_and_Eric's_Billion_Dollar_Movie     link    152
    * Dave_Kneebone   Tim_and_Eric's_Billion_Dollar_Movie     link    16
    * Will_Ferrell    Tim_and_Eric's_Billion_Dollar_Movie     link    302
    * other-internal  Tim_and_Eric's_Billion_Dollar_Movie     external        71
    * Michael_Gross_(actor)   Tim_and_Eric's_Billion_Dollar_Movie     link    12
    * Twink_Caplan    Tim_and_Eric's_Billion_Dollar_Movie     link    13
    * Tim_&_Eric      Tim_and_Eric's_Billion_Dollar_Movie     link    352
    * List_of_films_shot_in_Palm_Springs,_California  Tim_and_Eric's_Billion_Dollar_Movie     link    10
    * */
    public static class SecondDataPartMapper extends Mapper<Object, Text, Text, Text> {

        private final String separator = "\t";
        private Text key = new Text();
        private Text val = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(separator);
            this.key.set(parts[1] + separator + parts[2]);
            this.val.set(parts[3] + delimiter + toJoin);
            context.write(this.key, this.val);
        }
    }

    /*
    * Input data here is like:
    * Captain_Marvel_(film)   external  6190163#top
    * Tim_and_Eric's_Billion_Dollar_Movie     external        71#to_join
    * */
    public static class ReduceSideJoiner extends Reducer<Text, Text, Text, Text> {

        private Text count = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean isJoinable = false;
            int sum = 0;
            for (Text value: values) {
                String[] parts = value.toString().split(delimiter);
                if (parts[1].equals(top)) {
                    isJoinable = true;
                    continue;
                }
                if (parts[1].equals(toJoin)) {
                    sum += Integer.parseInt(parts[0]);
                }
            }
            if (isJoinable) {
                count.set(Integer.toString(sum));
                context.write(count, key);
            }
        }
    }
}
