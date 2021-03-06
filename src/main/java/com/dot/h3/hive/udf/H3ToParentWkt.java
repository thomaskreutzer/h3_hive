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

@Description(name = "H3ToParentWkt",
value = "_FUNC_(long index, integer resolution) - returns WKT of Parent\n "
+ "_FUNC_(string index, integer resolution) - returns WKT of Parent",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > CREATE TEMPORARY FUNCTION H3ToParentWkt AS 'com.dot.h3.hive.udf.H3ToParentWkt';\n"
+ "  > SELECT H3ToParentWkt(617733123174039551, 9) AS parent;\n"
+ "  > +----------------------------------------------+\n"
+ "  > |                    parent                    |\n"
+ "  > +----------------------------------------------+\n"
+ "  > | POINT(-73.90212095615803 40.86061876224212)  |\n"
+ "  > +----------------------------------------------+\n\n"

+ "Example 2:\n"
+ "  > CREATE TEMPORARY FUNCTION H3ToParentWkt AS 'com.dot.h3.hive.udf.H3ToParentWkt';\n"
+ "  > SELECT H3ToParentWkt('892a100acc7ffff', 9) AS parent;"
+ "  > +----------------------------------------------+\n"
+ "  > |                    parent                    |\n"
+ "  > +----------------------------------------------+\n"
+ "  > | POINT(-73.90212095615803 40.86061876224212)  |\n"
+ "  > +----------------------------------------------+\n\n")


public class H3ToParentWkt extends GenericUDF {
	private final Text strOut = new Text();
	PrimitiveObjectInspector inputOI0;
	PrimitiveObjectInspector inputOI1;
	H3Core h3;
	WKT wkt;
	
	public H3ToParentWkt() throws H3InstantiationException {
		wkt = new WKT();
		try {
			h3 = H3Core.newInstance();
		} catch (IOException e) {
			throw new H3InstantiationException("Could not instantiate the H3 core library");
		}
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		checkArgsSize(arguments, 2, 2);
		checkArgPrimitive(arguments, 0);
		inputOI0 = (PrimitiveObjectInspector)arguments[0];
		inputOI1 = (PrimitiveObjectInspector)arguments[1];
		if (! ( (inputOI0 instanceof StringObjectInspector)
				|| (inputOI0 instanceof LongObjectInspector) )
			) {
			throw new UDFArgumentException("Currently only string and long can be passed into H3ToParentWkt for the first parameter.\n" 
					+ "The type passed in to argument 0 is " + inputOI0.getPrimitiveCategory().name() + "\n"
					+ "The type passed in to argument 1 is " + inputOI1.getPrimitiveCategory().name());
		}
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object arg0 = arguments[0].get(); //Index
		Object arg1 = arguments[1].get(); //Resolution
		
		if (arg0 == null) return null;
		if (arg1 == null) return null;
		
		Integer res = (Integer) inputOI1.getPrimitiveJavaObject(arg1);
		
		if(inputOI0.getPrimitiveCategory().name() == "STRING") {
			String indexStr = (String) inputOI0.getPrimitiveJavaObject(arg0);
			strOut.set( wkt.geoCoordToPointWkt( h3.h3ToGeo( h3.h3ToParentAddress(indexStr, res) ) ) );
		} else {
			Long indexL = (Long) inputOI0.getPrimitiveJavaObject(arg0);
			strOut.set( wkt.geoCoordToPointWkt( h3.h3ToGeo( h3.h3ToParent(indexL, res) ) ) );
		}
		return strOut;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("H3ToParentWkt", children, ",");
	}
}
