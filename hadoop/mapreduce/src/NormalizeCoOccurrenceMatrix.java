import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NormalizeCoOccurrenceMatrix {

    public static class NormalizeMatrixMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input productA:productB \t relation
            //output productB \t productA:relation

            String[] tokens = value.toString().trim().split("\t");
            String[] products = tokens[0].split(":");

            int product1 = Integer.parseInt(products[0]);
            int product2 = Integer.parseInt(products[1]);
            int relation = Integer.parseInt(tokens[1]);

            context.write(new IntWritable(product2), new Text(product1 + ":" + relation));
        }
    }

    public static class NormalizeMatrixReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {
        // reduce method
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<Integer, Double> productRelationMap = new HashMap<Integer, Double>();

            double sum = 0;
            //product1:relation
            while(values.iterator().hasNext()) {
                String[] tokens = values.iterator().next().toString().trim().split(":");
                int product1 = Integer.parseInt(tokens[0]);
                double relation = Double.parseDouble(tokens[1]);
                sum += relation;
                productRelationMap.put(product1, relation);
            }

            for(Map.Entry<Integer, Double> entry: productRelationMap.entrySet()) {
                double normalizedRelation = entry.getValue() / sum;
                DecimalFormat df = new DecimalFormat("#.0000");
                normalizedRelation = Double.valueOf(df.format(normalizedRelation));
                context.write(new Text(entry.getKey() + ":" + key), new DoubleWritable(normalizedRelation));
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMatrixMapper.class);
        job.setReducerClass(NormalizeMatrixReducer.class);

        job.setJarByClass(NormalizeCoOccurrenceMatrix.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

}
