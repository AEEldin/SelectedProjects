import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mapperJob2 extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		String line = "";
		while (itr.hasMoreTokens()) {
			String word = itr.nextToken().toString();
			line += word;
			line += "\t";
			if (word.equals("$")) {
				String[] columnDetail = line.split("\t");
				context.write(new Text(columnDetail[0]), new Text(line));
				line = "";
			}
		}
	}
}
