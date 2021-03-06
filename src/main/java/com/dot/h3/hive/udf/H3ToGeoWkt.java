package com.dot.h3.hive.udf;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import com.dot.h3.exceptions.H3InstantiationException;
import com.dot.h3.util.WKT;
import com.uber.h3core.H3Core;

import org.apache.hadoop.io.Text;

@Description(name = "H3ToGeoWkt",
value = "_FUNC_(long index) - returns WKT POINT of the lat/long"
+"_FUNC_(string index) - returns WKT POINT of the lat/long",
extended = "Returns NULL if any argument is NULL.\n"
		+ "Example:\n"
		+ "  > CREATE TEMPORARY FUNCTION H3ToGeoWkt AS 'com.dot.h3.hive.udf.H3ToGeoWkt';\n"
		+ "  > SELECT H3ToGeoWkt(61773312317403955) AS wkt;\n"
		+ "  > +------------------------------------------------+\n"
		+ "  > |                      wkt                       |\n"
		+ "  > +------------------------------------------------+\n"
		+ "  > | POINT(-105.89054624819013 -30.32377110841559)  |\n"
		+ "  > +------------------------------------------------+\n\n"

		+ "Example 2:\n"
		+ "  > CREATE TEMPORARY FUNCTION H3ToGeoWkt AS 'com.dot.h3.hive.udf.H3ToGeoWkt';\n"
		+ "  > SELECT H3ToGeoWkt('892a100acc7ffff') AS wkt;"
		+ "  > +------------------------------------------------+\n"
		+ "  > |                      wkt                       |\n"
		+ "  > +------------------------------------------------+\n"
		+ "  > | POINT(-105.89054624819013 -30.32377110841559)  |\n"
		+ "  > +------------------------------------------------+\n\n")




public class H3ToGeoWkt extends GenericUDF {
	private final Text strOut = new Text();
	PrimitiveObjectInspector inputOI0;
	H3Core h3;
	WKT wkt;
	
	public H3ToGeoWkt() throws H3InstantiationException {
		wkt = new WKT();
		try {
			h3 = H3Core.newInstance();
		} catch (IOException e) {
			throw new H3InstantiationException("Could not instantiate the H3 core library");
		}
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		checkArgsSize(arguments, 1, 1);
		checkArgPrimitive(arguments, 0);
		inputOI0 = (PrimitiveObjectInspector)arguments[0];
		
		if (! ( (inputOI0 instanceof StringObjectInspector)
				|| (inputOI0 instanceof LongObjectInspector) )
			) {
			throw new UDFArgumentException("Currently only string and long can be passed into H3ToGeoWkt for the argument.\n" 
					+ "The type passed in to argument 0 is " + inputOI0.getPrimitiveCategory().name() );
		}
		
		ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
		return outputOI;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object arg0 = arguments[0].get(); //Index
		if (arg0 == null) return null;
		if(inputOI0.getPrimitiveCategory().name() == "STRING") {
			String indexStr = (String) inputOI0.getPrimitiveJavaObject(arg0);
			strOut.set( wkt.geoCoordToPointWkt( h3.h3ToGeo(indexStr) ) );
		} else {
			Long index = (Long) inputOI0.getPrimitiveJavaObject(arg0);
			strOut.set( wkt.geoCoordToPointWkt( h3.h3ToGeo(index) ) );
		}
		return strOut;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("H3ToGeoWkt", children, ",");
	}
}
