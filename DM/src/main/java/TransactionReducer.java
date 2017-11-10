import java.io.IOException;
import java.util.Iterator;

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

public class TransactionReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>, JobConfigurable{
	
	private int maxItemNum;
	
	public void configure(JobConf conf){ 
        String s = conf.get("maxItemNum"); 
        this.maxItemNum = Integer.parseInt(s);
	} 

	public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> output, Reporter r) throws IOException {
		String resultStr = "";
		int i = 0;

		while(value.hasNext()) {
			if(i >= maxItemNum) break;
			resultStr += value.next().toString() + RetailAnalysis.SEPARATOR;//summarize all counts
			i++;
		}
		
		//output: [key: invoice_no, value: product_code]
		output.collect(key, new Text(resultStr));
	}
}
