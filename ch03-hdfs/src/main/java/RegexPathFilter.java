import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * RegexPathFilter操作类
 *
 * @author JackPan
 * @date 2022/06/21 14:28
 **/
public class RegexPathFilter implements PathFilter {


    private final String regex;
    private final boolean include;

    public RegexPathFilter(String regex) {
        this(regex, true);
    }

    public RegexPathFilter(String regex, boolean include) {
        this.regex = regex;
        this.include = include;
    }

    @Override
    public boolean accept(Path path) {
        return (path.toString().matches(regex)) ? include : !include;
    }
}
