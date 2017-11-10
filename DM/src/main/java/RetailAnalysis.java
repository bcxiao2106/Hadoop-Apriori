import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * Author: BICHENG XIAO
 * UCID: bx34
 * Email: bx34@njit.edu
 * */

public class RetailAnalysis extends Configured implements Tool {
	public static String SEPARATOR = ";";

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new RetailAnalysis(), args);
	    System.exit(res);
	}

	public int run(String[] args) throws Exception {
		
		//Check the user input
		if(args.length != 6){
			System.out.println("Please make sure both INPUT and OUTPUT dir are provided");
			System.out.println("Usage: hadoop jar DM.jar RetailAnalysis [1-INPUT_DIR] [2-OUTPUT_DIR] [3-TOTAL_TRANS] [4-MAX_ITEM] [5-MIN_SUP] [6-MIN_CONF]");
			System.exit(0);
		} 
		String INPUT_DIR = args[0];
		String OUTPUT_DIR = args[1];
		int totalTransaction = Integer.parseInt(args[2]);
		int maxItemNum = Integer.parseInt(args[3]);
		float minSupport = Float.parseFloat(args[4]);
		float minConfidence = Float.parseFloat(args[5]);
		
		//Word Counting Map-Reduce: original file => ["TransactionId","ItemSet"]
		JobConf cnfg = new JobConf(RetailAnalysis.class);
		FileSystem fs= FileSystem.get(cnfg); 
		//get the FileStatus list from the directory
		FileStatus[] statesList = fs.listStatus(new Path(INPUT_DIR));
		if(statesList != null){
		    for(FileStatus statesFile : statesList){
		        //add each file to the list of inputs for the map-Reduce job
		        FileInputFormat.addInputPath(cnfg, statesFile.getPath());
		    }
		}
		FileOutputFormat.setOutputPath(cnfg, new Path(OUTPUT_DIR+"/Transaction"));
		
		cnfg.setInt("totalTransaction", totalTransaction);
		cnfg.setInt("maxItemNum", maxItemNum);
		cnfg.setFloat("minSupport", minSupport);
		cnfg.setFloat("minConfidence", minConfidence);
	      
		cnfg.setMapperClass(TransactionMapper.class);
		cnfg.setReducerClass(TransactionReducer.class);
		cnfg.setMapOutputKeyClass(Text.class);
		cnfg.setMapOutputValueClass(Text.class);
		cnfg.setOutputKeyClass(Text.class);
		cnfg.setOutputValueClass(Text.class);
		//cnfg.set("mapred.textoutputformat.ignoreseparator", "true");  
		//cnfg.set("mapred.textoutputformat.separator", ",");//use comma as the separator
		
		RunningJob job = JobClient.runJob(cnfg);
		job.waitForCompletion();
		
		//Ranking Map-Reduce: [<"keyword", "state">, <"count">] => top(sort_by_count([<"keyword">, <"state", "count">]),3)
		if (job.isSuccessful()) {
			JobConf cnfgSupport = new JobConf(RetailAnalysis.class);
			fs = FileSystem.get(cnfgSupport); 
			//get FileStatus list from WordCountReducer's output dir
			statesList = fs.listStatus(new Path(OUTPUT_DIR+"/Transaction"));
			if(statesList != null){
			    for(FileStatus status : statesList){
			        //add each file to the list of inputs for the Map-Reduce job
			    	if (status.getLen() > 0) {
			    		FileInputFormat.addInputPath(cnfgSupport, status.getPath());
					} 
			    }
			}
			FileOutputFormat.setOutputPath(cnfgSupport, new Path(OUTPUT_DIR+"/Frequent"));
			
			cnfgSupport.setInt("totalTransaction", totalTransaction);
			cnfgSupport.setInt("maxItemNum", maxItemNum);
			cnfgSupport.setFloat("minSupport", minSupport);
			cnfgSupport.setFloat("minConfidence", minConfidence);
		      
			cnfgSupport.setMapperClass(SupportMapper.class);
			cnfgSupport.setReducerClass(SupportReducer.class);
			cnfgSupport.setInputFormat(KeyValueTextInputFormat.class);
			cnfgSupport.setMapOutputKeyClass(Text.class);
			cnfgSupport.setMapOutputValueClass(IntWritable.class);
			cnfgSupport.setOutputKeyClass(Text.class);
			cnfgSupport.setOutputValueClass(IntWritable.class);
			//cnfgSupport.set("mapred.textoutputformat.ignoreseparator", "true");  
			//cnfgSupport.set("mapred.textoutputformat.separator", ",");//use comma as the separator
			job = JobClient.runJob(cnfgSupport);
			job.waitForCompletion();
		}
			
		if (job.isSuccessful()) {
			JobConf cnfgAssociation = new JobConf(RetailAnalysis.class);
			fs = FileSystem.get(cnfgAssociation); 
			//get FileStatus list from WordCountReducer's output dir
			statesList = fs.listStatus(new Path(OUTPUT_DIR+"/Frequent"));
			if(statesList != null){
				for(FileStatus status : statesList){
				        //add each file to the list of inputs for the Map-Reduce job
				    if (status.getLen() > 0) {
				    	FileInputFormat.addInputPath(cnfgAssociation, status.getPath());
					} 
				}
			}
			FileOutputFormat.setOutputPath(cnfgAssociation, new Path(OUTPUT_DIR+"/Association"));
			//DistributedCache.addCacheFile(new Path(OUTPUT_DIR+"/Frequent/part-00000").toUri(), cnfgAssociation);
			cnfgAssociation.setInt("totalTransaction", totalTransaction);
			cnfgAssociation.setInt("maxItemNum", maxItemNum);
			cnfgAssociation.setFloat("minSupport", minSupport);
			cnfgAssociation.setFloat("minConfidence", minConfidence);
			      
			cnfgAssociation.setMapperClass(AssociationMapper.class);
			cnfgAssociation.setReducerClass(AssociationReducer.class);
			cnfgAssociation.setInputFormat(KeyValueTextInputFormat.class);
			cnfgAssociation.setMapOutputKeyClass(Text.class);
			cnfgAssociation.setMapOutputValueClass(IntWritable.class);
			cnfgAssociation.setOutputKeyClass(Text.class);
			cnfgAssociation.setOutputValueClass(Text.class);
			//cnfgAssociation.set("mapred.textoutputformat.ignoreseparator", "true");  
			//cnfgAssociation.set("mapred.textoutputformat.separator", ",");//use comma as the separator
			job = JobClient.runJob(cnfgAssociation);
			job.waitForCompletion();
		}
		return 0;
	}
	
}
