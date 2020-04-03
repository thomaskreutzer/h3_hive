package com.dot.h3.hive.udf;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import com.dot.h3.exceptions.H3InstantiationException;
import com.dot.h3.util.WKT;
import com.uber.h3core.H3Core;


@Description(name = "KRingToWkt",
value = "_FUNC_(long index, integer resolution) - returns WKT of Parent\n "
+ "_FUNC_(string index, integer resolution) - returns WKT of Parent",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > CREATE TEMPORARY FUNCTION KRingToWkt AS 'com.dot.h3.hive.udf.KRingToWkt';\n"
+ "  > SELECT KRingToWkt(617733123174039551, 9) AS KRingToWkt;\n"
+ "  > +----------------------------------------------------+\n"
+ "  > |                     kringtowkt                     |\n"
+ "  > +----------------------------------------------------+\n"
+ "  > | [\"POINT(-73.90212095615803 40.86061876224212)\",  |\n"
+ "  > +----------------------------------------------------+\n\n"


+ "Example 2:\n"
+ "  > CREATE TEMPORARY FUNCTION KRingToWkt AS 'com.dot.h3.hive.udf.KRingToWkt';\n"
+ "  > SELECT KRingToWkt('892a100acc7ffff', 9) AS KRingToWkt;"
+ "  > +----------------------------------------------------+\n"
+ "  > |                     kringtowkt                     |\n"
+ "  > +----------------------------------------------------+\n"
+ "  > | [\"POINT(-73.90212095615803 40.86061876224212)\",  |\n"
+ "  > +----------------------------------------------------+\n\n")

public class KRingToWkt extends GenericUDF {
	private final ArrayList<Text> resultText = new ArrayList<Text>();
	PrimitiveObjectInspector inputOI0;
	PrimitiveObjectInspector inputOI1;
	H3Core h3;
	WKT wkt;
	
	public KRingToWkt() throws H3InstantiationException {
		wkt = new WKT();
		try {
			h3 = H3Core.newInstance();
		} catch (IOException e) {
			throw new H3InstantiationException("Could not instantiate the H3 core library");
		}
	}
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		inputOI0 = (PrimitiveObjectInspector) arguments[0];
		inputOI1 = (PrimitiveObjectInspector)arguments[1];
		
		checkArgsSize(arguments, 2, 2);
		checkArgPrimitive(arguments, 0);
		
		//Check to ensure the value is string or long only!
		if (! ( (inputOI0 instanceof StringObjectInspector)
				|| (inputOI0 instanceof LongObjectInspector) )
			) {
			throw new UDFArgumentException("Currently only string and long can be passed into KRingToWkt for the first parameter.\n" 
					+ "The type passed in to argument 0 is " + inputOI0.getPrimitiveCategory().name() + "\n"
					+ "The type passed in to argument 1 is " + inputOI1.getPrimitiveCategory().name());
		}
		
		//Return the respective array
		return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object arg0 = arguments[0].get(); //index - Either Long or String
		Object arg1 = arguments[1].get(); //Resolution
		List<Long> indexLongArr = new ArrayList<Long>();
		List<String> indexStrArr = new ArrayList<String>();
		
		if (arg0 == null) return null;
		if (arg1 == null) return null;
		
		Integer res = (Integer) inputOI1.getPrimitiveJavaObject(arg1);
		
		if(inputOI0.getPrimitiveCategory().name() == "STRING") {
			String indexStr = (String) inputOI0.getPrimitiveJavaObject(arg0);
			indexStrArr = h3.kRing(indexStr,res);
			for(int i=0; i < indexStrArr.size(); i++) {
				resultText.add( new Text( wkt.geoCoordToPointWkt( h3.h3ToGeo( indexStrArr.get(i) ) ) ) );
			}
		} else {
			Long indexL = (Long) inputOI0.getPrimitiveJavaObject(arg0);
			indexLongArr = h3.kRing(indexL,res);
			for (int x = 0; x < indexLongArr.size(); x++) {
				resultText.add( new Text( wkt.geoCoordToPointWkt( h3.h3ToGeo( indexLongArr.get(x) ) ) ) );
			}
		}
		
		return resultText;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("KRingToWkt", children, ",");
	}



}
