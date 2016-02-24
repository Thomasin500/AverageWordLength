package solution;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  //convert Text Object value to a string object
	  String line = value.toString();
	  
	  //split the line into individual lines
	  for (String word : line.split("\\W+")) {
		  
	      if (word.length() > 0) {
	        
	        /*
	         * Call the write method on the Context object to emit a key
	         * and a value from the map method.
	         */
	        context.write(new Text(word.substring(0,1)), new IntWritable(word.length()));
	      }
	  }
  }
}
