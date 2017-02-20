
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class reducerJob5 extends Reducer<FloatWritable, Text, Text, FloatWritable> {

		@Override
		public void reduce(FloatWritable rank, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Iterator<Text> ite = values.iterator();
			while(ite.hasNext()) {
				Text pageTitle = ite.next();
				context.write(pageTitle, rank);
			}
		}
	}