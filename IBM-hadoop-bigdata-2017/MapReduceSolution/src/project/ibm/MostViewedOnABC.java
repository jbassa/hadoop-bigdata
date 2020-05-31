package project.ibm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*code written by @Joel BASSA*/
/*mapJoinSide is used here*/

/*problem statement
------------------
.what is the  most viewed show on ABC channel?
*/

/*instructions
--------------
.put all join2_gennum*.txt files in a directory to be loaded in distributed cache i.e /ibm/cache/
.put all join2_genchan*.txt files in another directory to be use as Map reduce input i.e /ibm/data/genchan/
.execute program using preferable method i.e hadoop jar MapReduceSolution.jar project.ibm.MostViewedOnABC /ibm/data/genchan/ /ibm/data/output
*/

public class MostViewedOnABC {
	
	
	/*mapper class*/
	public static class MapJoinMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text outputKey = new Text();
		private Text outputValue = new Text(); 
		
		Map<String, Integer> viewersCountInfo = new HashMap<>();
		
		
		/*group data, sum and load  into distributed cache*/
		protected void setup(Context context) throws IOException{
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path p: files){
			
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while(line != null){
						String[] tokens = line.split(",");
						String show = tokens[0];
						Integer viewersCount = Integer.valueOf(tokens[1]);
						
						if(viewersCountInfo.containsKey(show)){ 
							/*If the map previously contained a mapping for the key, the old value is replaced.*/
							viewersCountInfo.put(show, viewersCountInfo.get(show) + viewersCount  );
						}
						else{
							viewersCountInfo.put(show, viewersCount);
						}
						line = reader.readLine();
					}
					reader.close();
				
			}
		}
		
		/*map function*/
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] tokens = line.split(",");
			String show = tokens[0];
			String channel = tokens[1];
			
			if(channel.equals("ABC")){
				outputKey.set(channel);
				outputValue.set(show+ " "+ viewersCountInfo.get(show).toString());
				context.write(outputKey, outputValue);	
				
			}
		}		
	}
	
	  /*reducer*/
	public static class Reduce extends Reducer<Text, Text, Text, Text> {  
		       public void reduce(Text key, Iterable<Text> values, Context context) 
		        throws IOException, InterruptedException {
		         int max = 0, viewersNumber;
		         String mostViewedShow = null ;
		         for (Text val : values) {
		        	 String[] tokens = val.toString().split(" "); //tokens[0]=>showValue  token[1] ==>viewersNumber
		        	 viewersNumber = Integer.valueOf(tokens[1]);
		        	 if (viewersNumber > max) {
		              max = viewersNumber;
		              mostViewedShow = tokens[0];
		         }
		        
		       }
		       context.write(key, new Text(mostViewedShow));
	}	    }
	
	/*driver*/
	public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", " ");
		Job job = new Job(conf, "most_viewed_show_ABC");
	    job.setJarByClass( MostViewedOnABC.class );
	    job.setMapperClass( MapJoinMapper.class );
	    job.setReducerClass( Reduce.class);
	    DistributedCache.addCacheFile(new URI("/ibm/cache/join2_gennumA.txt"), job.getConfiguration());
	    DistributedCache.addCacheFile(new URI("/ibm/cache/join2_gennumB.txt"), job.getConfiguration());
	    DistributedCache.addCacheFile(new URI("/ibm/cache/join2_gennumC.txt"), job.getConfiguration());
	    job.setMapOutputKeyClass( Text.class );
	    job.setMapOutputValueClass( Text.class );
	    job.setOutputKeyClass( Text.class );	    
	    job.setOutputValueClass( Text.class );
	    
	    FileInputFormat.addInputPath( job, new Path( args[0] ) );
	    FileOutputFormat.setOutputPath( job, new Path( args[1] ) );
	    
	    System.exit( job.waitForCompletion( true ) ? 0 : 1 );
	}

}
