import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduce {

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(
                Object key,
                Text value,
                Context context
        ) throws IOException, InterruptedException {
            // Splitting the input string into tokens
            String[] tokens = value.toString().split(";");
            // Extracting the list of ingredients
            String[] ingredients = tokens[5].split(": ")[1].split(", ");

            boolean isVegetarian = tokens[1].trim().equals("Yes");
            boolean isNotComplicated = ingredients.length <= 4;

            if (isVegetarian && isNotComplicated) {
                String category = tokens[4].trim();
                int prepTime = Integer.parseInt(tokens[2].trim().split(" ")[0]);

                // Emitting the output
                context.write(new Text(category), new IntWritable(prepTime));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            int count = 0;
            int totalTime = 0;

            // Calculating the average preparation time
            for (IntWritable v : values) {
                totalTime += v.get();
                count++;
            }
            if (count > 0) {
                int averagePrepTime = totalTime / count;
                // Emitting the average category preparation time
                context.write(key, new IntWritable(averagePrepTime));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "MapReduce");

        job.setJarByClass(MapReduce.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}