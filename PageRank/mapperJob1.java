import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class mapperJob1 extends Mapper<LongWritable, Text, Text, Text> {
    
    private static final Pattern wikiLinksPattern = Pattern.compile("\\[.+?\\]");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] titleAndText = parseTitleAndText(value);
        String pageString = titleAndText[0];
        //if(pageString.contains(":"))
        //    return;
        Text page = new Text(pageString.replace(' ', '_'));
        context.write(page, new Text("!"));
        
        Matcher matcher = wikiLinksPattern.matcher(titleAndText[1]);
        while (matcher.find()) {
            String otherPage = matcher.group();
            otherPage = getWikiPageFromLink(otherPage);
            if(otherPage == null || otherPage.isEmpty()) 
                continue;
            context.write(new Text(otherPage.trim()), page);
        }
    }
    
    private String[] parseTitleAndText(Text value) throws CharacterCodingException {
        String[] titleAndText = new String[2];
        int start = value.find("<title>");
        int end = value.find("</title>", start);
        start += 7; //add <title> length.
        titleAndText[0] = Text.decode(value.getBytes(), start, end-start);
        start = value.find("<text");
        start = value.find(">", start);
        end = value.find("</text>", start);
        start += 1;
        if(start == -1 || end == -1) {
            return new String[]{titleAndText[0],""};
        }
        titleAndText[1] = Text.decode(value.getBytes(), start, end-start);
        return titleAndText;
    }

    private String getWikiPageFromLink(String aLink){
        if(isNotWikiLink(aLink)) return null;
        int start = aLink.startsWith("[[") ? 2 : 1;
        int endLink = aLink.indexOf("]");
        int pipePosition = aLink.indexOf("|");
        if(pipePosition > 0){
            endLink = pipePosition;
        }
        aLink =  aLink.substring(start, endLink);
        aLink = aLink.replaceAll(" ", "_");
        if(aLink.contains("&amp;"))
            return aLink.replace("&amp;", "&");
        return aLink;
    }
    
    

    private boolean isNotWikiLink(String aLink) {
        int start = 1;
        if(aLink.startsWith("[[")){
            start = 2;
        }
        //if( aLink.length() < 2 || aLink.length() > 100) return true;
        if( aLink.length() > 100) return true;
        
        char firstChar = aLink.charAt(start);
        if( firstChar == ':') return true;
        if( firstChar == ',') return true;
        if( firstChar == ')') return true;
        if( firstChar == '{') return true;
        if( aLink.contains("#")) return true;
        if( aLink.contains("File:")) return true;
        if( aLink.contains("Category:")) return true;
        if( aLink.contains("Help:")) return true;
        if( aLink.contains("fr:")) return true;
        if( aLink.startsWith("../")) return true;
        if( aLink.endsWith(" ")) return true;
        return false;
    }
}