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

public class TransactionMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text>{
    
	public void map(Object key, Text value, OutputCollector<Text,Text> output, Reporter r) throws IOException {

		String[] text = value.toString().split(",");
		if(!text[0].equals("InvoiceNo") && text[1].length() != 0){
			output.collect(new Text(text[0]), new Text(text[1]));
		}
	}
}
