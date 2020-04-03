package com.dot.h3.hive.udf;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

import com.dot.h3.exceptions.H3InstantiationException;
import com.dot.h3.util.WKT;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;

import org.apache.hadoop.io.Text;

@Description(name = "PolyfillToArrayWkt",
value = "_FUNC_(string plygon_wkt, string holes_poly_multipoly, integer resolution) - returns an array of points as WKT",
extended = "Returns NULL if polygon_wkt or resultion arguments are NULL.\n"

+ "Example:\n"
+ "  > SELECT _FUNC_('POLYGON((-71.23094863399959 42.35171702149799,-71.20507841890782 42.39384377360396,-71.18534241583312 42.40583588152941,-71.13489748711537 42.40374196572458,-71.12786523200806 42.3537116038451,-71.23094863399959 42.35171702149799))', null, 9) AS WKT;\n"
+ "  Returns Array<String> \n"
+ "Can take either NULL, MULTIPOLYGON or POLYGON WKT for the holes_poly_multipoly argument.\n"
+ " The resolution can be between 0 and 15, 15 is the most granular")


public class PolyfillToArrayWkt extends GenericUDF {
	private final ArrayList<Text> result = new ArrayList<Text>();
	PrimitiveObjectInspector inputOI0;
	PrimitiveObjectInspector inputOI1;
	PrimitiveObjectInspector inputOI2;
	H3Core h3;
	WKT wkt;
	
	public PolyfillToArrayWkt() throws H3InstantiationException {
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
		
		return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object arg0 = arguments[0].get(); //wkt
		Object arg1 = arguments[1].get(); //Holes wkt
		Object arg2 = arguments[2].get(); //Resolution
		List<Long> indexArr = new ArrayList<Long>();
		
		if (arg0 == null) return null;
		//Argument two will allow Nulls
		if (arg2 == null) return null;
		
		String wktStr = (String) inputOI0.getPrimitiveJavaObject(arg0);
		//Transform the wkt's to GeoCoord
		List<GeoCoord> geoPolygon = wkt.wktPolygonToGeoCoord(wktStr);
		
		//Get the resolution variable
		Integer res = (Integer) inputOI2.getPrimitiveJavaObject(arg2);
		
		
		if (arg1 != null) {
			String holes = (String) inputOI1.getPrimitiveJavaObject(arg1);
			List<List<GeoCoord>> geoPolygonHoles = wkt.wktMultiPolygonToGeoCoord(holes);
			indexArr = h3.polyfill(geoPolygon, geoPolygonHoles, res);
		} else {
			indexArr = h3.polyfill(geoPolygon, null, res);
		}
		
		for (int x = 0; x < indexArr.size(); x++) {
			String wktPoint = wkt.geoCoordToPointWkt( h3.h3ToGeo( indexArr.get(x) ) );
			Text strOut = new Text();
			strOut.set( wktPoint );
			result.add( strOut );
		}
		
		return result;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("PolyfillToArrayWkt", children, ",");
	}
}
