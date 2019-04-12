package com.gisfederal.gpudb.processors.GPUdbNiFi;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.nifi.logging.ComponentLog;

import com.gpudb.ColumnProperty;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.Type.Column;
import com.gpudb.protocol.HasTableResponse;

public class KineticaUtilities {
    

    public static boolean checkForTimeStamp( Column column) throws Exception {
        boolean isTimeStamp = false;

        if ( column.hasProperty( ColumnProperty.TIMESTAMP ) ) {
            isTimeStamp = true;
        }

        return isTimeStamp;
    }

    public static String convertStacktraceToString(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String exceptionAsString = sw.toString();

        return exceptionAsString;
    }
    
    public static boolean tableExists(GPUdb gpudb, String tableName, ComponentLog logger) {
        HasTableResponse response;

        try {
            response = gpudb.hasTable(tableName, null);
            return response.getTableExists();
        } catch (GPUdbException ex) {
            logger.error("Failed checking if table exists in Kinetica");
        }
        
        return false;
    }

    public static Long parseDate(String dateString, String dataFormat, String timeZone, ComponentLog logger) throws Exception {
        if (dateString == null || dataFormat == null) {
            logger.error("Date and Patterns must not be null");
        }

        TimeZone timezone = timeZone == null ? TimeZone.getDefault() : TimeZone.getTimeZone(timeZone);
        
        SimpleDateFormat parser = new SimpleDateFormat();
        parser.setLenient(true);
        parser.applyPattern(dataFormat);
        parser.setTimeZone(timezone);
        try {
            Date date = parser.parse(dateString);
            if (date != null) {
                return date.getTime();
            }
        } catch (Exception e) {
            logger.error("FAILED to parse date " + "Data String: " + dateString + " Pattern: " + dataFormat +
                    " Timezone: " + timeZone.toString());
            logger.error(convertStacktraceToString(e));
        }

        return null;
    }

}
