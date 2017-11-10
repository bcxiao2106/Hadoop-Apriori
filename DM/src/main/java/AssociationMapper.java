import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;

/*
 * Author: BICHENG XIAO
 * UCID: bx34
 * Email: bx34@njit.edu
 * */

public class AssociationMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable>{
    
	public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter r) throws IOException {
		String[] text = key.toString().split(RetailAnalysis.SEPARATOR);
		if(text.length > 1){
			//output: [key: itemsets, value: support]
			output.collect(new Text(key.toString()), new IntWritable(Integer.parseInt(value.toString())));
		}
	}
}

