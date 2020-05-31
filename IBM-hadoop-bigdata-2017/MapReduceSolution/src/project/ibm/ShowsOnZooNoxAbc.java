package project.ibm;


import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*code written by @Joel BASSA*/
/*let use Reduce Join here as specified in the requirements :) */
/*Multiple Inputs will be used */

/*problem statement
------------------
.what are the aired shows on ZOO, NOX, ABC channels ?
*/

/*instructions
--------------
.put all join2_genchan*.txt files in a  directory to be use as Map reduce input i.e /ibm/data/genchan/
.no need to use join2_genmum*.txt files
.Please note 3 inputs are needed for running the program 
.execute program using preferable method i.e hadoop jar MapReduceSolution.jar project.ibm.ShowsOnZooNoxAbc /ibm/data/genchan/join2_genchanA.txt /ibm/data/genchan/join2_genchanB.txt /ibm/data/genchan/join2_genchanC.txt /ibm/data/output
*/

public class ShowsOnZooNoxAbc {
	
	
	/*mapper class*/
	/*handle join2_genchenA.txt*/
	/* NullWritable for having distinct values */
	public static class GenChenAMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		private Text outputKey = new Text();
		/*map function*/
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] tokens = line.split(",");
			String show = tokens[0];
			String channel = tokens[1];
			if(channel.equals("ZOO") || channel.equals("NOX") || channel.equals("ABC")){
				//put one space separator and interchange show and channel value
				//so that in reducer the same keys will be sorted by First channels characters
				outputKey.set(channel + " " +show);                                     
				context.write(outputKey, NullWritable.get());	
				
			}
		}		
	}
	
	/*mapper class*/
	/*handle join2_genchenB.txt*/
	public static class GenChenBMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		private Text outputKey = new Text();
		/*map function*/
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] tokens = line.split(",");
			String show = tokens[0];
			String channel = tokens[1];
			if(channel.equals("ZOO") || channel.equals("NOX") || channel.equals("ABC")){
				//put one space separator and interchange show and channel value
				//so that in reducer the same keys will be sorted by First channels characters
				outputKey.set(channel + " " +show);                                     
				context.write(outputKey, NullWritable.get());	
				
			}
		}		
	}
	
	
	/*mapper class*/
	/*handle join2_genchenC.txt*/
	public static class GenChenCMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		private Text outputKey = new Text();
		/*map function*/
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] tokens = line.split(",");
			String show = tokens[0];
			String channel = tokens[1];
			if(channel.equals("ZOO") || channel.equals("NOX") || channel.equals("ABC")){
				//put one space separator and interchange show and channel value
				//so that in reducer the same keys will be sorted by First channels characters
				outputKey.set(channel + " " +show);                                     
				context.write(outputKey, NullWritable.get());	
				
			}
		}		
	}

	  /*reducer*/
	public static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {  
		       public void reduce(Text key, Iterable<NullWritable> values, Context context) 
		        throws IOException, InterruptedException {
		    	   
		        	context.write(new Text(key), NullWritable.get());
		         }           
		    
	}	    
	
	/*driver*/
	public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		//conf.set("mapred.textoutputformat.separator", " ");
		Job job = new Job(conf, "Shows_On_Zoo_Nox_Abc");
	    job.setJarByClass( ShowsOnZooNoxAbc.class );
	    job.setReducerClass( Reduce.class);
	    
	    job.setMapOutputKeyClass( Text.class );
	    job.setMapOutputValueClass( NullWritable.class );
	    job.setOutputKeyClass( Text.class );	    
	    job.setOutputValueClass( NullWritable.class );
	    
	    MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, GenChenAMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, GenChenBMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[2]),TextInputFormat.class, GenChenCMapper.class);
	    Path outputPath = new Path(args[3]);


	    FileOutputFormat.setOutputPath(job, outputPath);
	   // outputPath.getFileSystem(conf).delete(outputPath);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
