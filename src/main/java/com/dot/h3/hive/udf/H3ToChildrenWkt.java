package com.dot.h3.hive.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

import com.dot.h3.exceptions.H3InstantiationException;
import com.dot.h3.util.WKT;
import com.uber.h3core.H3Core;

import org.apache.hadoop.io.Text;

@Description(name = "H3ToChildrenWkt",
value = "_FUNC_(long index, integer resolution) - returns long of Children\n "
+ "_FUNC_(string index, integer resolution) - returns string of Children",
extended = "Returns NULL if any argument is NULL. This returns the center point of the index. \n"
+ "Example:\n"
+ "  > CREATE TEMPORARY FUNCTION H3ToChildrenWkt AS 'com.dot.h3.hive.udf.H3ToChildrenWkt';\n"
+ "  > SELECT H3ToChildrenWkt(599718724986994687, 9) AS children;\n"
+ "  > +----------------------------------------------------+\n"
+ "  > |                      children                      |\n"
+ "  > +----------------------------------------------------+\n"
+ "  > | [\"POINT(-73.99191613398102 40.85293293570688)\",\"POINT(-73.98966951517899 40.85034641308286)\",\n"
+ "  > +----------------------------------------------------+\n\n"

+ "Example 2:\n"
+ "  > CREATE TEMPORARY FUNCTION H3ToChildrenWkt AS 'com.dot.h3.hive.udf.H3ToChildrenWkt';\n"
+ "  > SELECT H3ToChildrenWkt('852a100bfffffff', 9) AS children;"
+ "  > +----------------------------------------------------+\n"
+ "  > |                      children                      |\n"
+ "  > +----------------------------------------------------+\n"
+ "  > | [\"POINT(-73.99191613398102 40.85293293570688)\",\"POINT(-73.98966951517899 40.85034641308286)\",\n"
+ "  > +----------------------------------------------------+\n\n")

public class H3ToChildrenWkt extends GenericUDF {
	private final ArrayList<Text> resultText = new ArrayList<Text>();
	PrimitiveObjectInspector inputOI0;
	PrimitiveObjectInspector inputOI1;
	H3Core h3;
	WKT wkt;
	
	public H3ToChildrenWkt() throws H3InstantiationException {
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
			throw new UDFArgumentException("Currently only string and long can be passed into H3ToChildrenWkt for the first parameter.\n" 
					+ "The type passed in to argument 0 is " + inputOI0.getPrimitiveCategory().name() + "\n"
					+ "The type passed in to argument 1 is " + inputOI1.getPrimitiveCategory().name());
		}
		return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object arg0 = arguments[0].get(); //Index
		Object arg1 = arguments[1].get(); //Resolution
		List<Long> indexLongArr = new ArrayList<Long>();
		List<String> indexStrArr = new ArrayList<String>();
		
		if (arg0 == null) return null;
		if (arg1 == null) return null;
		
		Integer res = (Integer) inputOI1.getPrimitiveJavaObject(arg1);
		
		if(inputOI0.getPrimitiveCategory().name() == "STRING") {
			String indexStr = (String) inputOI0.getPrimitiveJavaObject(arg0);
			indexStrArr = h3.h3ToChildren(indexStr,res);
			for(int i=0; i < indexStrArr.size(); i++) {
				String wktPoint = wkt.geoCoordToPointWkt( h3.h3ToGeo( indexStrArr.get(i) ) );
				Text strOut = new Text();
				strOut.set( wktPoint );
				resultText.add( strOut );
			}
			return resultText;
		} else {
			Long indexL = (Long) inputOI0.getPrimitiveJavaObject(arg0);
			indexLongArr = h3.h3ToChildren(indexL,res);
			for (int x = 0; x < indexLongArr.size(); x++) {
				String wktPoint = wkt.geoCoordToPointWkt( h3.h3ToGeo( indexLongArr.get(x) ) );
				Text strOut = new Text();
				strOut.set( wktPoint );
				resultText.add( strOut );
			}
		}
		return resultText;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("H3ToChildrenWkt", children, ",");
	}
}
