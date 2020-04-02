package com.dot.h3.util;

import java.util.ArrayList;
import java.util.List;

import com.uber.h3core.util.GeoCoord;

public class WKT {
	
	public String geoCoordToPolygonWkt(List<GeoCoord> gc){
		StringBuilder builder=new StringBuilder();
		builder.append("POLYGON((");
		for (int i = 0; i < gc.size(); i++) {
			if(i>0){
				builder.append(",");
			}
			builder.append( gc.get(i).lng + " " + gc.get(i).lat );
		}
		builder.append("))");
		return builder.toString();
	}
	
	public String polygonWkt(double[] latlon){
		if(latlon.length%2 != 0)
			throw new IllegalArgumentException("latlon length is not even");
			StringBuilder builder=new StringBuilder();
			builder.append("POLYGON((");
			for(int i=0;i<latlon.length;i+=2){
			if(i>0){
				builder.append(",");
			}
			builder.append(latlon[i]+" "+latlon[i+1]);
		}
		builder.append("))");
		return builder.toString();
	}
	
	public String geoCoordToPointWkt(GeoCoord gc) {
		StringBuilder builder=new StringBuilder();
		builder.append("POINT(");
		builder.append( gc.lng + " " + gc.lat );
		builder.append(")");
		return builder.toString();
	}
	
	public List<GeoCoord> wktPolygonToGeoCoord(String wkt) {
		List<GeoCoord> lgc = new ArrayList<GeoCoord>();
		wkt = wkt.replace("POLYGON((", "").replace("POINT(", "").replace("))", "").replace(")", "");
		String[] splt = wkt.split(",");
		for (int i = 0; i < splt.length; i++) {
			String[] longLat = splt[i].split(" "); //Split on the space
			lgc.add( new GeoCoord( Double.parseDouble(longLat[0]), Double.parseDouble(longLat[1]) ) );
		}
		return lgc;
	}
	
	
	public List<List<GeoCoord>> wktMultiPolygonToGeoCoord(String wkt) {
		List<List<GeoCoord>> lgc = new ArrayList<List<GeoCoord>>();
		List<String> polygons;
		
		String multipolygonParsed = "";
		if(wkt.contains("MULTIPOLYGON")) {
			multipolygonParsed = wkt.replace("MULTIPOLYGON(", "").replace(")))", "").replace("\n", "").replace("\r", "");
			polygons = cleanPolygonsArr( multipolygonParsed.split("\\),\\(") );
			//Loop each of the polygons returned as the List<String>
			for (int i = 0; i < polygons.size(); i++) {
				List<GeoCoord> gc = new ArrayList<GeoCoord>();
				String[] longLatArr = polygons.get(i).split(",");
				for(int x = 0; x < longLatArr.length; x++) {
					String[] longLat = longLatArr[x].split(" "); //Split on the space
					gc.add( new GeoCoord( Double.parseDouble(longLat[0]), Double.parseDouble(longLat[1]) ) ); 
				}
				//Add the list of coordinates to the list
				lgc.add(gc);
			}
		} else {
			//Only Polygon
			lgc.add(wktPolygonToGeoCoord(wkt));
		}
		return lgc;
	}
	
	
	private List<String> cleanPolygonsArr(String[] polygon) {
		List<String> r = new ArrayList<String>();
		for (int i = 0; i < polygon.length; i++) {
			r.add (polygon[i].replaceAll("\\)", "").replaceAll("\\(", "") );
		}
		return r;
	}
	

}
