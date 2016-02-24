package solution;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	/* 
	 * this method will receive a key (first letter) and value(the length of each word)
	 * that the mapper output
	 *
	*/
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

	  	double length = 0.0;
	  	int words = 0;
	  
   
	  	for (IntWritable value : values) {
		  
	  		length += value.get();
	  		words++;
		}
	  	
	  	length = length / words;
	  	
	  	System.out.println(key + "    " + length);
	  	
	  	
	  	/*
		 * Call the write method on the Context object to emit a key
		 * and a value from the reduce method. 
		 */
	  	context.write(key, new DoubleWritable(length));

  }
}