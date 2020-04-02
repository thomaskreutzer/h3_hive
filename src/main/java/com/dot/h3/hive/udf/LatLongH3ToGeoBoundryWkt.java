package com.dot.h3.hive.udf;


import java.io.IOException;
import java.util.List;

import com.dot.h3.exceptions.*;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import com.dot.h3.util.WKT;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;

import org.apache.hadoop.io.Text;

@Description(name = "LatLongH3ToGeoBoundryWkt",
value = "_FUNC_(lat double, long double, resolution integer) - returns a WKT",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > SELECT _FUNC_(40.86016, -73.90071, 12);\n"
+ "  POLYGON((-73.90218697935661 40.862381901482266,-73.9042969767565 40.86144407471913,-73.90423087546569 40.85968095579108,-73.90205493792557 40.858855661723865,-73.89994501590182 40.85979341878112,-73.90001095604163 40.86155653960862))\n"
+ " The resolution can be between 0 and 15, 15 is the most granular")

public class LatLongH3ToGeoBoundryWkt extends GenericUDF {
	private final Text strOut = new Text();
	PrimitiveObjectInspector inputOI0;
	PrimitiveObjectInspector inputOI1;
	PrimitiveObjectInspector inputOI2;
	WKT wkt;
	H3Core h3;
	
	public LatLongH3ToGeoBoundryWkt() throws H3InstantiationException {
		wkt = new WKT();
		try {
			h3 = H3Core.newInstance();
		} catch (IOException e) {
			throw new H3InstantiationException("Could not instantiate the H3 core library");
		}
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		checkArgsSize(arguments, 3, 3);
		checkArgPrimitive(arguments, 0);
		
		inputOI0 = (PrimitiveObjectInspector)arguments[0];
		inputOI1 = (PrimitiveObjectInspector)arguments[1];
		inputOI2 = (PrimitiveObjectInspector)arguments[2];
		
		ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
		return outputOI;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object arg0 = arguments[0].get(); //Latitude
		Object arg1 = arguments[1].get(); //Longitude
		Object arg2 = arguments[2].get(); //Resolution
		
		if (arg0 == null) return null;
		if (arg1 == null) return null;
		if (arg2 == null) return null;
		
		Double lat = (Double) inputOI0.getPrimitiveJavaObject(arg0);
		Double lon = (Double) inputOI1.getPrimitiveJavaObject(arg1);
		Integer res = (Integer) inputOI2.getPrimitiveJavaObject(arg2);
		
		List<GeoCoord> cords = h3.h3ToGeoBoundary( h3.geoToH3(lat, lon, res) );
		
		//Add the first GeoCoord in the polygon to be the last GeoCoord to close it properly on a map.
		cords.add( new GeoCoord( cords.get(0).lng, cords.get(0).lat ) );
		
		strOut.set( wkt.geoCoordToPolygonWkt(cords) );
		return strOut;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("LatLongH3ToGeoBoundryWkt", children, ",");
	}
}
