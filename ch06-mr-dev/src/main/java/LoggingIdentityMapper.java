
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * LoggingIdentityMapper
 * @author jackpan
 * @param <KEYIN>
 * @param <VALUEIN>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public class LoggingIdentityMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    private static final Log LOG = LogFactory.getLog(LoggingIdentityMapper.class);

    @Override
    public void map(KEYIN key, VALUEIN value, Context context)
            throws IOException, InterruptedException {

        // Log to stdout file
        System.out.println("Map key: " + key);

        // Log to syslog file
        LOG.info("Map key: " + key);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Map value: " + value);
        }

        context.write((KEYOUT) key, (VALUEOUT) value);
    }
}
