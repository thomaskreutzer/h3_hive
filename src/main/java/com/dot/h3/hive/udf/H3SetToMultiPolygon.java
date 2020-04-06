package com.dot.h3.hive.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;



import com.dot.h3.exceptions.H3InstantiationException;
import com.dot.h3.util.WKT;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;

@Description(name = "H3SetToMultiPolygon",
value = "_FUNC_(array(long index,long index)) - returns an array of WKT String\n "
+ "_FUNC_(array(string index, string index)) - returns returns an array of WKT String",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > CREATE TEMPORARY FUNCTION H3SetToMultiPolygon AS 'com.dot.h3.hive.udf.H3SetToMultiPolygon';\n"
+ "  > SELECT H3SetToMultiPolygon(array(617733122422996991,617733122423259135)) AS wkt_array;\n"
+ "  > +----------------------------------------------------+\n"
+ "  > |                     wkt_array                      |\n"
+ "  > +----------------------------------------------------+\n"
+ "  > | [\"POLYGON((-73.99184674763184 40.851169994189114,-73.99177736650066 40.84940709139823,-73.98960021718895 40.84858352856829,-73.98749236284553 40.849522800877686,-73.98756158290558 40.85128570584109,-73.98973881838064 40.852109336325405,-73.98980812679454 40.85387229829392,-73.99198552554876 40.85469591594949,-73.99409352970484 40.85375650398033,-73.99402406020228 40.85199354419126,-73.99184674763184 40.851169994189114))\",\"POLYGON((-73.99184674763184 40.851169994189114,-73.99177736650066 40.84940709139823,-73.98960021718895 40.84858352856829,-73.98749236284553 40.849522800877686,-73.98756158290558 40.85128570584109,-73.98973881838064 40.852109336325405,-73.98980812679454 40.85387229829392,-73.99198552554876 40.85469591594949,-73.99409352970484 40.85375650398033,-73.99402406020228 40.85199354419126,-73.99184674763184 40.851169994189114))\"] |\n"
+ "  > +----------------------------------------------------+\n\n"

+ "Example 2:\n"
+ "  > CREATE TEMPORARY FUNCTION H3SetToMultiPolygon AS 'com.dot.h3.hive.udf.H3SetToMultiPolygon';\n"
+ "  > SELECT H3SetToMultiPolygon(array('892a1008003ffff','892a1008007ffff')) AS wkt_array;"
+ "  > +----------------------------------------------------+\n"
+ "  > |                     wkt_array                      |\n"
+ "  > +----------------------------------------------------+\n"
+ "  > |                 same as the above                  |\n"
+ "  > +----------------------------------------------------+\n\n")


public class H3SetToMultiPolygon extends GenericUDF {
	private final ArrayList<Text> result = new ArrayList<Text>();
	ListObjectInspector listInputObjectInspector;
	H3Core h3;
	WKT wkt;
	
	public H3SetToMultiPolygon() throws H3InstantiationException {
		wkt = new WKT();
		try {
			h3 = H3Core.newInstance();
		} catch (IOException e) {
			throw new H3InstantiationException("Could not instantiate the H3 core library");
		}
	}
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		checkArgsSize(arguments, 1, 1); // This UDF accepts one argument
		assert(arguments[0].getCategory() == Category.LIST); // The first argument is a list

		listInputObjectInspector = (ListObjectInspector)arguments[0];

		/*
		 * Here comes the real usage for Object Inspectors : we know that our return
		 * type is equal to the type of the elements of the input array. We don't need
		 * to know in details what this type is, the ListObjectInspector already has it
		 */
		//return listInputObjectInspector.getListElementObjectInspector();
		return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}
	
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		
		Object arg0 = arguments[0].get();
		if (arg0 == null) return null;
		
		int numberOfElements = listInputObjectInspector.getListLength(arg0);
		//System.out.println("Number of Elements " + numberOfElements);
		
		if(listInputObjectInspector.getTypeName().contains("array<string>")) {
			Collection<Long> c = new HashSet<Long>();
			
			for (int i = 0; i < numberOfElements; i++) {
				Long l = h3.stringToH3( listInputObjectInspector.getListElement(arg0, i).toString() );
				c.add( l );
			}
			List<List<List<GeoCoord>>> geoCoordLists = h3.h3SetToMultiPolygon(c, true);
			ArrayList<String> ret =  wkt.h3SetToMultiPolygonWktArray(geoCoordLists);
			for (int i = 0; i < ret.size(); i++) {
				result.add( new Text( ret.get(i)) );
			}
		} else {
			Collection<Long> c = new HashSet<Long>();
			
			for (int i = 0; i < numberOfElements; i++) {
				c.add( Long.parseLong( listInputObjectInspector.getListElement(arg0, i).toString() ) );
			}
			List<List<List<GeoCoord>>> geoCoordLists = h3.h3SetToMultiPolygon(c, true);
			ArrayList<String> ret =  wkt.h3SetToMultiPolygonWktArray(geoCoordLists);
			for (int i = 0; i < ret.size(); i++) {
				result.add( new Text(ret.get(i)) );
			}
		}
		
		return result;
	
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("H3SetToMultiPolygon", children, ",");
	}

}


