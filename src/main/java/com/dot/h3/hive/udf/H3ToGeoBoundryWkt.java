package com.dot.h3.hive.udf;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import com.dot.h3.exceptions.H3InstantiationException;
import com.dot.h3.util.WKT;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;

@Description(name = "H3ToGeoBoundryWkt",
value = "_FUNC_(long index) - returns WKT Polygon\n"
+ "_FUNC_(string index) - returns WKT Polygon\\",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > CREATE TEMPORARY FUNCTION H3ToGeoBoundryWkt AS 'com.dot.h3.hive.udf.H3ToGeoBoundryWkt';\n"
+ "  > SELECT H3ToGeoBoundryWkt(61773312317403955) AS wkt;\n"
+ "  > +----------------------------------------------------+\n"
+ "  > |                        wkt                         |\n"
+ "  > +----------------------------------------------------+\n"
+ "  > | POLYGON((-105.89053610304362 -30.323807809188516,  |\n"
+ "  > +----------------------------------------------------+\n\n"

+ "Example 2:\n"
+ "  > CREATE TEMPORARY FUNCTION H3ToGeoBoundryWkt AS 'com.dot.h3.hive.udf.H3ToGeoBoundryWkt';\n"
+ "  > SELECT H3ToGeoBoundryWkt('892a100acc7ffff') AS wkt;"
+ "  > +----------------------------------------------------+\n"
+ "  > |                        wkt                         |\n"
+ "  > +----------------------------------------------------+\n"
+ "  > | POLYGON((-105.89053610304362 -30.323807809188516,  |\n"
+ "  > +----------------------------------------------------+\n\n")




public class H3ToGeoBoundryWkt extends GenericUDF {
	private final Text strOut = new Text();
	PrimitiveObjectInspector inputOI0;
	H3Core h3;
	WKT wkt;
	
	public H3ToGeoBoundryWkt() throws H3InstantiationException {
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
			throw new UDFArgumentException("Currently only string and long can be passed into H3ToGeoBoundryWkt for the argument.\n" 
					+ "The type passed in to argument 0 is " + inputOI0.getPrimitiveCategory().name() );
		}
		ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
		return outputOI;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object arg0 = arguments[0].get(); //Index
		String out;
		if (arg0 == null) return null;
		if(inputOI0.getPrimitiveCategory().name() == "STRING") {
			String indexStr = (String) inputOI0.getPrimitiveJavaObject(arg0);
			List<GeoCoord> gcoord = h3.h3ToGeoBoundary(indexStr);
			out = wkt.geoCoordToPolygonWkt(gcoord);
		} else {
			Long index = (Long) inputOI0.getPrimitiveJavaObject(arg0);
			List<GeoCoord> gcoord = h3.h3ToGeoBoundary(index);
			out = wkt.geoCoordToPolygonWkt(gcoord);
		}
		strOut.set( out );
		return strOut;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("H3ToGeoBoundryWkt", children, ",");
	}
}
