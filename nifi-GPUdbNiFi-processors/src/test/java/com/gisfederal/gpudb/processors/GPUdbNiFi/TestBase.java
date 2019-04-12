package com.gisfederal.gpudb.processors.GPUdbNiFi;

import com.gpudb.GPUdb;
import com.gpudb.protocol.ClearTableRequest;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;



public class TestBase {
    public static GPUdb gpudb;

    private final static Logger LOG = Logger.getLogger( TestBase.class );

    private final static Map<String, String> clearTableOptions = GPUdb.options( ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS,
                                                                                ClearTableRequest.Options.TRUE );

    protected static String m_userGivenHosts;
    
    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void init() throws Exception {
        // Parse any user given URL(s)
        m_userGivenHosts = System.getProperty("url", "http://127.0.0.1:9191");
        LOG.debug("Given GPUdb URL(s): " + m_userGivenHosts );

        // Parse if the given URL string has to be split into a list
        boolean splitHostString = Boolean.parseBoolean( System.getProperty("list-constructor", "false") );

        // Split the host string, if the user says so
        if ( splitHostString ) {
            LOG.debug("Splitting the given URL(s): " + m_userGivenHosts );
            List<URL> urls = new ArrayList<URL>();
            String[] host_strings = m_userGivenHosts.split(",");
            for (int i = 0; i < host_strings.length; ++i ) {
                urls.add( new URL( host_strings[i] ) );
            }
            // Create a DB handle with the list
            gpudb = new GPUdb( urls );
        }
        else {
            LOG.debug("NOT splitting the given URL(s): " + m_userGivenHosts );
            // Create a DB handle with the string
            gpudb = new GPUdb( m_userGivenHosts );
        }

        
        LOG.debug("Established connetion to GPUdb at: " + gpudb.getURL().toString() );
    }

    
    public String generateTableName() {
        return testName.getMethodName() + "_" + UUID.randomUUID().toString();
    }


    public void clearTable( String tableName ) throws Exception {
        ClearTableRequest request = new ClearTableRequest();
        request.setTableName( tableName );
        request.setOptions( clearTableOptions );
        // request.setOptions( GPUdb.options( ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS,
        //                                    ClearTableRequest.Options.TRUE ) );
        gpudb.clearTable( request );
    }
}  // end TestBase
