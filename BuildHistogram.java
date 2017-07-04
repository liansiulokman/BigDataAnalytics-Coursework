package polyu.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import polyu.bigdata.PartitionCounting.PartitionMapper;
import polyu.bigdata.PartitionCounting.PartitionReducer;



public class BuildHistogram {
	public static void main(String[] args) throws Exception {
        // Check the input parameters
        if (args.length != 5) {
            System.out.println("Please input correct parameters");
            return;
        } else if (Integer.parseInt(args[0])>Integer.parseInt(args[1])) {
            System.out.println("Invalid dataset range definition");
            return;
        } else if ((Integer.parseInt(args[1])-Integer.parseInt(args[0]))%Integer.parseInt(args[2])!=0){
        	System.out.println("Invalid num_partition parameter setting");
        	return;
        }
        
        // Read the parameters and store them
        
        Configuration conf = new Configuration();
        conf.set("min_value", args[0]);
        conf.set("max_value", args[1]);
        conf.set("num_partitions", args[2]);
        
        
        // Initialize job related information
        Job job1 = Job.getInstance(conf, "Partition Counting");
        
        //insert your codes here
        System.exit(job1.waitForCompletion(true) ? 0 : 1);

    }

}
