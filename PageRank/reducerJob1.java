import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class reducerJob1 extends Reducer<Text, Text, Text, Text> {
	
	public static int countSubstring(String subStr, String str) {
		return (str.length() - str.replace(subStr, "").length())
				/ subStr.length();
	}
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String pagerank = "";
		boolean first = true;
		for (Text value : values) {
			if (!first)
				pagerank += "##";
			pagerank += value.toString();
			first = false;
		}

		if (pagerank.contains("!")) {
			context.write(key, new Text(""+" $"));
			String result[] = pagerank.split("##");
			for (String s : result) {
				if ((!s.equals("!")) && (!s.equals(key.toString())))
					context.write(new Text(s), new Text(key+" $"));
			}
		}
	}
}