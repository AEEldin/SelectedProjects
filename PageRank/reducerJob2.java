import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class reducerJob2 extends Reducer<Text, Text, Text, Text> {

	public static int countSubstring(String subStr, String str) {
		return (str.length() - str.replace(subStr, "").length())
				/ subStr.length();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String pagerank = "";
		String finalLink = "";
		boolean first = true;
		boolean second = true;
		for (Text value : values) {
			if (!first)
				pagerank += "\t";
			pagerank += value.toString();
			first = false;
		}

		String result[] = pagerank.split("\t");
		for (String s : result) {
			if ((!s.equals(key.toString())) && (!s.equals("$"))
					&& (!s.equals("\t")) && (!s.equals(" "))) {
				if ((s.length() > 1) && (!(finalLink.contains(s)))) {
					if (!second)
						finalLink += "\t";
					finalLink += s.toString();
					second = false;
				}
			}
		}

		context.write(key, new Text(finalLink));
	}
}
