import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CoOccurrenceMatrixGenerator {
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//value = userid \t product1: rating, product2: rating...
			//key = product1: product2 value = 1
			String line = value.toString().trim();
			String[] user_productRatings = line.split("\t");
			String user = user_productRatings[0];
			String[] product_ratings = user_productRatings[1].split(",");
			//{product1:rating, product2:rating..}
			for(int i = 0; i < product_ratings.length; i++) {
				String product1 = product_ratings[i].trim().split(":")[0];
				
				for(int j = 0; j < product_ratings.length; j++) {
					String product2 = product_ratings[j].trim().split(":")[0];
					context.write(new Text(product1 + ":" + product2), new IntWritable(1));
				}
			}
			
		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//key product1:product2 value = iterable<1, 1, 1>

			// if the calculated score is smaller than the threshold, do not write this result out in order to optimize
			// the speed of the program

			int threshold = 10;
			int sum = 0;
			while(values.iterator().hasNext()) {
				sum += values.iterator().next().get();
			}

			if(sum > threshold) {
				context.write(key, new IntWritable(sum));
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);
		
		job.setJarByClass(CoOccurrenceMatrixGenerator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}
