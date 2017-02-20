import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public  class reducerPreprocessJob4 extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String numberofnodes = conf.get("NumberOfNodes");
		float N = Float.valueOf(numberofnodes);
		float initValue = 1.0f/N;

		String out = Float.toString(initValue) + "@";
		//TODO: use values.decode instead:
		Iterator<Text> ite = values.iterator();
		while(ite.hasNext()) {
			Text t = ite.next();
			out = out + t.toString();
		}

		context.write(page, new Text(out));
	}
}