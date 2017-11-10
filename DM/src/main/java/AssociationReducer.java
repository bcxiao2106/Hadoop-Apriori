import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/*
 * Author: BICHENG XIAO
 * UCID: bx34
 * Email: bx34@njit.edu
 * */

public class AssociationReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text>, JobConfigurable{
	
	private HashMap<String, Integer> map = new HashMap<String, Integer>();
	private float minConfidence;
	private int totalTransaction;
	//private float minSupport;
	
	public void configure(JobConf conf){ 
		
		String c = conf.get("minConfidence"); 
        String s = conf.get("totalTransaction"); 
        
        this.minConfidence = Float.parseFloat(c);
        this.totalTransaction = Integer.parseInt(s);
        String dictFile = conf.get("fs.defaultFS") + "/user/ubuntu/output/Frequent/part-00000";
        
        try {
        	Path pt = new Path(dictFile);
        	FileSystem fs = FileSystem.get(conf);
        	BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(pt)));
        	String currLine = null;	
        	while ((currLine = fis.readLine()) != null) {
        		String[] items = currLine.split("\t");
        		if(items.length == 2){
        			map.put(items[0], Integer.parseInt(items[1]));
        		}
        	}
        	fis.close();
        	fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}  
	} 

	public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, Text> output, Reporter r) throws IOException {
		String[] items = key.toString().split(RetailAnalysis.SEPARATOR);
		String outputKey = "";
		String outputValue = "";
		int implyingItemSetSupport;
		int fullItemsetSupport = map.get(key.toString());
		int confidence;
		
		while(value.hasNext()){
			value.next();
		if(items.length >= 2){
			for(int i = items.length - 1; i >= 0; i--){
				String impliedItem = items[i];
				String implyingItems = "";
				int count = 1;
				for(int j = 0; j < items.length; j++){
					if(j != i){
						implyingItems += items[j];
						if(count < items.length - 1)  implyingItems += RetailAnalysis.SEPARATOR;
						count++;
					}
				}

				if(map.containsKey(implyingItems)){
					implyingItemSetSupport = map.get(implyingItems);
					confidence = (int) (fullItemsetSupport * 1.0 / implyingItemSetSupport * 100);
					
					if(confidence >= this.minConfidence * 100){
						outputKey = implyingItems + " -> " + impliedItem;
						outputValue = "["+ String.format("%.2f", (fullItemsetSupport *1.0 / this.totalTransaction * 100)) + "%, "+ confidence +"%]";
						output.collect(new Text(outputKey), new Text(outputValue));
					}
				}
			}	
		}
		}
	}
}
