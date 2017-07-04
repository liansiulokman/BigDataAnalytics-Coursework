package polyu.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PartitionCounting {
	

    public static class PartitionMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    	
        private final static IntWritable one = new IntWritable(1);
        private IntWritable part = new IntWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException { 
        //insert your codes here

            context.write(part,one);     
        }
    }
    public static class PartitionReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text range = new Text();
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
         //insert your codes here

            context.write(range, result);         
        }
    }
}

