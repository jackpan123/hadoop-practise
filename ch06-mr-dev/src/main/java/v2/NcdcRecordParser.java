package v2;

import org.apache.hadoop.io.Text;

/**
 * @author jackpan
 * @version v1.0 2021/9/23 13:35
 */
public class NcdcRecordParser {

    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemperature;
    private String quality;

    public void parse(String record) {
        year = record.substring(15, 19);
        String airTemperatureString;
        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
        } else {
            airTemperatureString = record.substring(87, 92);
        }

        airTemperature = Integer.parseInt(airTemperatureString);
        quality = record.substring(92, 93);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemperature() {
        return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }

    /**
     * Gets year.
     *
     * @return Value of year.
     */
    public String getYear() {
        return this.year;
    }

    /**
     * Gets airTemperature.
     *
     * @return Value of airTemperature.
     */
    public int getAirTemperature() {
        return this.airTemperature;
    }
}
