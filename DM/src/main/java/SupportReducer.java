import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/*
 * Author: BICHENG XIAO
 * UCID: bx34
 * Email: bx34@njit.edu
 * */

public class SupportReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
	
	private int totalTransaction;
	private float minSupport;
	
	public void configure(JobConf conf){ 
        String s = conf.get("totalTransaction"); 
        String m = conf.get("minSupport");
        
        this.totalTransaction = Integer.parseInt(s);
        this.minSupport = Float.parseFloat(m);
	} 

	public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, IntWritable> output, Reporter r) throws IOException {
		int supportValue = 0;

		while(value.hasNext()) {
			supportValue += value.next().get();//summarize all counts
		}

		//output: [key: itemsets, value: support]
		if(supportValue >= minSupport * totalTransaction && key.toString().length() != 0){
			output.collect(key, new IntWritable(supportValue));
		}
	}
}
