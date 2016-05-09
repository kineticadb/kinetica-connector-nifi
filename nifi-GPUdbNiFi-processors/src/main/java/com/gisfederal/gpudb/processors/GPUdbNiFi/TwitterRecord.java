package com.gisfederal.gpudb.processors.GPUdbNiFi;

import com.gpudb.ColumnProperty;
import com.gpudb.RecordObject;
import java.io.Serializable;

/**
 *
 * @author David Katz
 */

public class TwitterRecord extends RecordObject implements Serializable {
        @RecordObject.Column(order = 0, properties = { ColumnProperty.DATA })
	public String AUTHOR;

        @RecordObject.Column(order = 1)
	public String TEXT;
        
        @RecordObject.Column(order = 2)
	public long TIMESTAMP;

        @RecordObject.Column(order = 3)
	public String URL;

        @RecordObject.Column(order = 4)
        public double x;

        @RecordObject.Column(order = 5)
        public double y; 
        
        public TwitterRecord(){
            
        }
        
        public TwitterRecord(float x, float y, long timestamp, String author, String url, String text){
            this.x = x;
            this.y = y;
            this.TIMESTAMP = timestamp;
            this.TEXT = text;
            this.AUTHOR = author;
            this.URL = url;
        }
}
