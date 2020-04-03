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

@Description(name = "KRingDistances",
value = "_FUNC_(long index, integer noOfRings) - returns WKT of multipolygon rings\n "
+ "_FUNC_(string index, integer noOfRings) - returns WKT of multipolygon rings",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > CREATE TEMPORARY FUNCTION KRingDistances AS 'com.dot.h3.hive.udf.KRingDistances';\n"
+ "  > SELECT KRingDistances(631243922056054783, 9) AS wkt;\n"
+ "  > +----------------------------------------------------+\n"
+ "  > |                        wkt                         |\n"
+ "  > +----------------------------------------------------+\n"
+ "  > | MULTIPOLYGON(((-73.90074702414034 40.86016857340853))\n"
+ "  > +----------------------------------------------------+\n\n"


+ "Example 2:\n"
+ "  > CREATE TEMPORARY FUNCTION KRingDistances AS 'com.dot.h3.hive.udf.KRingDistances';\n"
+ "  > SELECT KRingDistances('8c2a100acc687ff', 9) AS wkt;"
+ "  > +----------------------------------------------------+\n"
+ "  > |                        wkt                         |\n"
+ "  > +----------------------------------------------------+\n"
+ "  > | MULTIPOLYGON(((-73.90074702414034 40.86016857340853))\n"
+ "  > +----------------------------------------------------+\n\n")


public class KRingDistances extends GenericUDF {
	PrimitiveObjectInspector inputOI0;
	PrimitiveObjectInspector inputOI1;
	H3Core h3;
	WKT wkt;
	
	public KRingDistances() throws H3InstantiationException {
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
		
		//Check to ensure the value is string or long only!
		if (! ( (inputOI0 instanceof StringObjectInspector)
				|| (inputOI0 instanceof LongObjectInspector) )
			) {
			throw new UDFArgumentException("Currently only string and long can be passed into KRingDistances for the first parameter.\n" 
					+ "The type passed in to argument 0 is " + inputOI0.getPrimitiveCategory().name() 
					+ "The type passed in to argument 1 is " + inputOI1.getPrimitiveCategory().name() );
		}
		
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object arg0 = arguments[0].get(); //Index
		Object arg1 = arguments[1].get(); //NumberOfRings
		
		if (arg0 == null) return null;
		if (arg1 == null) return null;
		
		Integer numOfRings = (Integer) inputOI1.getPrimitiveJavaObject(arg1);
		
		if(inputOI0.getPrimitiveCategory().name() == "STRING") {
			String indexStr = (String) inputOI0.getPrimitiveJavaObject(arg0);
			List<List<String>> krd = h3.kRingDistances(indexStr, numOfRings);
			return new Text( wkt.KringStringndexToMultiPolygon(krd, h3) );
		} else {
			Long indexL = (Long) inputOI0.getPrimitiveJavaObject(arg0);
			List<List<Long>> krd2 = h3.kRingDistances(indexL, numOfRings);
			return new Text( wkt.KringLongIndexToMultiPolygon(krd2, h3) );
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("KRingDistances", children, ",");
	}
}
