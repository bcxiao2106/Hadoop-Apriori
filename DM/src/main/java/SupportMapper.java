import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/*
 * Author: BICHENG XIAO
 * UCID: bx34
 * Email: bx34@njit.edu
 * */

public class SupportMapper extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable>{
    
	public void map(Text key, Text value, OutputCollector<Text,IntWritable> output, Reporter r) throws IOException {
		String[] text = value.toString().split(RetailAnalysis.SEPARATOR);
		//System.out.println(value.toString());
		ArrayList<String> itemList = new ArrayList<String>();
		for(int i = 0; i < text.length; i++){//remove duplicate
			if(itemList.contains(text[i])){
				continue;
			} else {
				itemList.add(text[i]);
			}
		}
		
		Collections.sort(itemList);
		
		CombinationUtil cbn = new CombinationUtil();
		ArrayList<String> ItemSetList = (ArrayList<String>)cbn.getCombination(itemList,1);
		
		for(int j = 0; j < ItemSetList.size(); j++){
			//output: [key: itemsets, value: support]
			output.collect(new Text(ItemSetList.get(j)), new IntWritable(1));
		}
	}
}
