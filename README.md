# h3_hive

H3 UDF's for Apache Hive



## Function List

1. EdgeLength
2. GeoToH3
3. GeoToH3Address
4. GetH3UnidirectionalEdge
5. GetH3UnidirectionalEdgesFromHexagon
6. H3Distance
7. H3IndexesAreNeighbors
8. H3SetToMultiPolygon
9. H3ToCenterChild
10. H3ToCenterChildWkt
11. H3ToChildren
12. H3ToChildrenWkt
13. H3ToGeoBoundaryWkt
14. H3ToGeoWkt
15. H3ToParent
16. H3ToParentWkt
17. H3ToString
18. HexArea
19. KRing
20. KRingDistances
21. KRingToWkt
22. LatLongH3ToGeoBoundaryWkt
23. NumHexagons
24. PolyfillToArrayH3Index
25. PolyfillToArrayWkt

For all of the functions listed below you execute describe.


```SQL
DESCRIBE FUNCTION <function_name>;
DESCRIBE FUNCTION EXTENDED <function_name>;
end
```

## Function Examples

**NOTE:** Temporary functions are used for these examples. Temporary functions will not work in LLAP.
You must create permanent functions in LLAP. 

### EdgeLength

```SQL
CREATE TEMPORARY FUNCTION EdgeLength AS 'com.dot.h3.hive.udf.EdgeLength';
SELECT _FUNC_(12, 'm') AS edge_meters;
+--------------+
| edge_meters  |
+--------------+
| 9.415526211  |
+--------------+
```

### GeoToH3

```SQL
CREATE TEMPORARY FUNCTION GeoToH3 AS 'com.dot.h3.hive.udf.GeoToH3';
SELECT GeoToH3(40.86016, -73.90071, 12) AS index;
+---------------------+
|        index        |
+---------------------+
| 631243922056054783  |
+---------------------+
```

### GeoToH3Address

```SQL
CREATE TEMPORARY FUNCTION GeoToH3Address AS 'com.dot.h3.hive.udf.GeoToH3Address';
SELECT GeoToH3Address(40.86016, -73.90071, 12) AS index;
+------------------+
|      index       |
+------------------+
| 892a100acc7ffff  |
+------------------+
```

### GetH3UnidirectionalEdge

**NOTE:** The indexes must be neighbors for this to work.

**BAD EXAMPLE**

```SQL
CREATE TEMPORARY FUNCTION GetH3UnidirectionalEdge AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdge';
SELECT GetH3UnidirectionalEdge(61773312317403955,631243922056054783) AS index;
Error: Error while compiling statement: FAILED: IllegalArgumentException Given indexes are not neighbors. (state=42000,code=40000)
```


**First Example:** neighbors that work

```SQL
CREATE TEMPORARY FUNCTION GetH3UnidirectionalEdge AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdge';
SELECT GetH3UnidirectionalEdge(617733122422996991,617733122423259135) AS edge;
 +----------------------+
 |         edge         |
 +----------------------+
 | 1266251468764348415  |
 +----------------------+
```

**Second Example:** neighbors that work from string:

```SQL
CREATE TEMPORARY FUNCTION GetH3UnidirectionalEdge AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdge';
SELECT GetH3UnidirectionalEdge('892a1008003ffff','892a1008007ffff') AS edge;"
 +-------------------+
 |        edge       |
 +-------------------+
 | 1192a1008003ffff  |
 +-------------------+
```


### GetH3UnidirectionalEdgesFromHexagon

**First Example:** 

```SQL
CREATE TEMPORARY FUNCTION gh3udefh AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdgesFromHexagon';
SELECT gh3udefh(599718724986994687) AS list;
+----------------------------------------------------+
|                        list                        |
+----------------------------------------------------+
| [1248237071328346111,1320294665366274047, etc.     |
+----------------------------------------------------+
```

**Second Example:** 

```SQL
CREATE TEMPORARY FUNCTION gh3udefh AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdgesFromHexagon';
SELECT gh3udefh('852a100bfffffff') AS list;"
+----------------------------------------------------+
|                        list                        |
+----------------------------------------------------+
| [\"1152a100bfffffff\",\"1252a100bfffffff\", etc.   |
+----------------------------------------------------+
```


### H3Distance

**NOTE:** The indexes should be same size? I had the error as noted below. 

```SQL
CREATE TEMPORARY FUNCTION H3Distance AS 'com.dot.h3.hive.udf.H3Distance';
SELECT H3Distance(61773312317403955,631243922056054783) AS dist;
Error: Error while compiling statement: FAILED: SemanticException [Error 10014]: Line 1:7 Wrong arguments '631243922056054783': org.apache.hadoop.hive.ql.metadata.HiveException:com.uber.h3core.exceptions.DistanceUndefinedException: Distance not defined between the two indexes. (state=42000,code=10014)
```

**Example:** neighbors that work

```SQL
CREATE TEMPORARY FUNCTION H3Distance AS 'com.dot.h3.hive.udf.H3Distance';
SELECT H3Distance(617733122422996991,617733122423259135) AS dist;
+-------+
| dist  |
+-------+
| 1     |
+-------+
```

### H3IndexesAreNeighbors

**BAD Example**

```SQL
CREATE TEMPORARY FUNCTION H3IndexesAreNeighbors AS 'com.dot.h3.hive.udf.H3IndexesAreNeighbors';
SELECT H3IndexesAreNeighbors('db768011473333','892a100acc7ffff') AS neighbors;
+------------+
| neighbors  |
+------------+
| false      |
+------------+
```

**Example** neighbors that work

```SQL
CREATE TEMPORARY FUNCTION H3IndexesAreNeighbors AS 'com.dot.h3.hive.udf.H3IndexesAreNeighbors';
SELECT H3IndexesAreNeighbors(617733122422996991,617733122423259135) AS neighbors;
+------------+
| neighbors  |
+------------+
| true       |
+------------+
```

### H3SetToMultiPolygon

**Example 1**

```SQL
CREATE TEMPORARY FUNCTION H3SetToMultiPolygon AS 'com.dot.h3.hive.udf.H3SetToMultiPolygon';
SELECT H3SetToMultiPolygon(array(617733122422996991,617733122423259135)) AS wkt_array;
+----------------------------------------------------+
|                     wkt_array                      |
+----------------------------------------------------+
| [\"POLYGON((-73.99184674763184 40.851169994189114,-73.99177736650066 40.84940709139823,-73.98960021718895 40.84858352856829,-73.98749236284553 40.849522800877686,-73.98756158290558 40.85128570584109,-73.98973881838064 40.852109336325405,-73.98980812679454 40.85387229829392,-73.99198552554876 40.85469591594949,-73.99409352970484 40.85375650398033,-73.99402406020228 40.85199354419126,-73.99184674763184 40.851169994189114))\",\"POLYGON((-73.99184674763184 40.851169994189114,-73.99177736650066 40.84940709139823,-73.98960021718895 40.84858352856829,-73.98749236284553 40.849522800877686,-73.98756158290558 40.85128570584109,-73.98973881838064 40.852109336325405,-73.98980812679454 40.85387229829392,-73.99198552554876 40.85469591594949,-73.99409352970484 40.85375650398033,-73.99402406020228 40.85199354419126,-73.99184674763184 40.851169994189114))\"] |
+----------------------------------------------------+
```

**Example 2**

```SQL
CREATE TEMPORARY FUNCTION H3SetToMultiPolygon AS 'com.dot.h3.hive.udf.H3SetToMultiPolygon';
SELECT H3SetToMultiPolygon(array('892a1008003ffff','892a1008007ffff')) AS wkt_array;"
+----------------------------------------------------+
|                     wkt_array                      |
+----------------------------------------------------+
|                 same as the above                  |
+----------------------------------------------------+)
```

### H3ToCenterChild


**Example 1**

```SQL
CREATE TEMPORARY FUNCTION H3ToCenterChild AS 'com.dot.h3.hive.udf.H3ToCenterChild'
SELECT H3ToCenterChild(61773312317403955, 13) AS center_child
+--------------------+
|    center_child    |
+--------------------+
| 61773312317403955  |
+--------------------+
```

**Example 2**

```SQL
CREATE TEMPORARY FUNCTION H3ToCenterChild AS 'com.dot.h3.hive.udf.H3ToCenterChild'
SELECT H3ToCenterChild('db768011473333', 13) AS center_child;
+-----------------+
|  center_child   |
+-----------------+
| db768011473333  |
+-----------------+
```


### H3ToCenterChildWkt


**Example 1**

```SQL
CREATE TEMPORARY FUNCTION H3ToCenterChildWkt AS 'com.dot.h3.hive.udf.H3ToCenterChildWkt'
SELECT H3ToCenterChildWkt(61773312317403955, 13) AS center_child_wkt;
+------------------------------------------------+
|                center_child_wkt                |
+------------------------------------------------+
| POINT(-105.89054624819013 -30.32377110841559)  |
+------------------------------------------------+
```

**Example 2**

```SQL
CREATE TEMPORARY FUNCTION H3ToCenterChildWkt AS 'com.dot.h3.hive.udf.H3ToCenterChildWkt';
SELECT H3ToCenterChildWkt('db768011473333', 13) AS center_child_wkt;"
+------------------------------------------------+
|                center_child_wkt                |
+------------------------------------------------+
| POINT(-105.89054624819013 -30.32377110841559)  |
+------------------------------------------------+
```


### H3ToChildren

**Example 1**

```SQL
CREATE TEMPORARY FUNCTION H3ToChildren AS 'com.dot.h3.hive.udf.H3ToChildren';
SELECT H3ToChildren(599718724986994687, 9) AS children;
+----------------------------------------------------+
|                      children                      |
+----------------------------------------------------+
| [617733122422996991,617733122423259135,617733122423521279, etc.. 
+----------------------------------------------------+
```

**Example 2**

```SQL
CREATE TEMPORARY FUNCTION H3ToChildren AS 'com.dot.h3.hive.udf.H3ToChildren';
SELECT H3ToChildren('852a100bfffffff', 9) AS children;"
+----------------------------------------------------+
|                      children                      |
+----------------------------------------------------+
| [\"892a1008003ffff\",\"892a1008007ffff\", etc...   |
+----------------------------------------------------+
```


### H3ToChildrenWkt

**Example 1**

```SQL
CREATE TEMPORARY FUNCTION H3ToChildrenWkt AS 'com.dot.h3.hive.udf.H3ToChildrenWkt';
SELECT H3ToChildrenWkt(599718724986994687, 9) AS children;
+----------------------------------------------------+
|                      children                      |
+----------------------------------------------------+
| [\"POINT(-73.99191613398102 40.85293293570688)\",\"POINT(-73.98966951517899 40.85034641308286)\",
+----------------------------------------------------+
```

**Example 2**

```SQL
CREATE TEMPORARY FUNCTION H3ToChildrenWkt AS 'com.dot.h3.hive.udf.H3ToChildrenWkt';
SELECT H3ToChildrenWkt('852a100bfffffff', 9) AS children;"
+----------------------------------------------------+
|                      children                      |
+----------------------------------------------------+
| [\"POINT(-73.99191613398102 40.85293293570688)\",\"POINT(-73.98966951517899 40.85034641308286)\",
+----------------------------------------------------+
```


### H3ToGeoBoundaryWkt

**Example 1**

```SQL
CREATE TEMPORARY FUNCTION H3ToGeoBoundaryWkt AS 'com.dot.h3.hive.udf.H3ToGeoBoundaryWkt';
SELECT H3ToGeoBoundaryWkt(61773312317403955) AS wkt;
+----------------------------------------------------+
|                        wkt                         |
+----------------------------------------------------+
| POLYGON((-105.89053610304362 -30.323807809188516,  |
+----------------------------------------------------+
```

**Example 2**

```SQL
CREATE TEMPORARY FUNCTION H3ToGeoBoundaryWkt AS 'com.dot.h3.hive.udf.H3ToGeoBoundaryWkt';
SELECT H3ToGeoBoundaryWkt('892a100acc7ffff') AS wkt;"
+----------------------------------------------------+
|                        wkt                         |
+----------------------------------------------------+
| POLYGON((-105.89053610304362 -30.323807809188516,  |
+----------------------------------------------------+
```



### H3ToGeoWkt

**Example 1**

```SQL
CREATE TEMPORARY FUNCTION H3ToGeoWkt AS 'com.dot.h3.hive.udf.H3ToGeoWkt';
SELECT H3ToGeoWkt(61773312317403955) AS wkt;
+------------------------------------------------+
|                      wkt                       |
+------------------------------------------------+
| POINT(-105.89054624819013 -30.32377110841559)  |
+------------------------------------------------+
```

**Example 2**

```SQL
CREATE TEMPORARY FUNCTION H3ToGeoWkt AS 'com.dot.h3.hive.udf.H3ToGeoWkt';
SELECT H3ToGeoWkt('892a100acc7ffff') AS wkt;"
+------------------------------------------------+
|                      wkt                       |
+------------------------------------------------+
| POINT(-105.89054624819013 -30.32377110841559)  |
+------------------------------------------------+
```


### H3ToParent

**Example 1**

```SQL
CREATE TEMPORARY FUNCTION H3ToParent AS 'com.dot.h3.hive.udf.H3ToParent';
SELECT H3ToParent(617733123174039551, 5) AS parent;
+---------------------+
|       parent        |
+---------------------+
| 599718724986994687  |
+---------------------+
```

**Example 2**

```SQL
CREATE TEMPORARY FUNCTION H3ToParent AS 'com.dot.h3.hive.udf.H3ToParent';
SELECT H3ToParent('892a100acc7ffff', 5) AS parent;"
+------------------+
|      parent      |
+------------------+
| 852a100bfffffff  |
+------------------+
```

### H3ToParentWkt

**Example 1**

```SQL
CREATE TEMPORARY FUNCTION H3ToParentWkt AS 'com.dot.h3.hive.udf.H3ToParentWkt';
SELECT H3ToParentWkt(617733123174039551, 9) AS parent;
+----------------------------------------------+
|                    parent                    |
+----------------------------------------------+
| POINT(-73.90212095615803 40.86061876224212)  |
+----------------------------------------------+
```

**Example 2**

```SQL
CREATE TEMPORARY FUNCTION H3ToParentWkt AS 'com.dot.h3.hive.udf.H3ToParentWkt';
SELECT H3ToParentWkt('892a100acc7ffff', 9) AS parent;"
+----------------------------------------------+
|                    parent                    |
+----------------------------------------------+
| POINT(-73.90212095615803 40.86061876224212)  |
+----------------------------------------------+
```


### H3ToString

**Example**

```SQL
CREATE TEMPORARY FUNCTION H3ToString AS 'com.dot.h3.hive.udf.H3ToString';
SELECT H3ToString(631243922056054783);
8c2a100acc687ff
```

### HexArea

**Example 1**

```SQL
CREATE TEMPORARY FUNCTION HexArea as 'com.dot.h3.hive.udf.HexArea';
SELECT HexArea(9, 'km2') AS hex_area;
+------------+
|  hex_area  |
+------------+
| 0.1053325  |
+------------+
```

**Example 2**

```SQL
CREATE TEMPORARY FUNCTION HexArea as 'com.dot.h3.hive.udf.HexArea';
SELECT HexArea(9, 'm2') AS hex_area;
+-----------+
| hex_area  |
+-----------+
| 105332.5  |
+-----------+
```




### KRing

**Example 1**

```SQL
CREATE TEMPORARY FUNCTION KRing AS 'com.dot.h3.hive.udf.KRing';
SELECT KRing(617733123174039551, 9) AS kring;
+----------------------------------------------+
|                    kring                     |
+----------------------------------------------+
| [617733123174039551,617733123173777407, etc. |
+----------------------------------------------+
```

**Example 2**

```SQL
CREATE TEMPORARY FUNCTION KRing AS 'com.dot.h3.hive.udf.KRing';
SELECT KRing('892a100acc7ffff', 9) AS kring;"
+----------------------------------------------+
|                    kring                     |
+----------------------------------------------+
| [\"892a100acc7ffff\",\"892a100acc3ffff\",etc.|
+----------------------------------------------+
```



### KRing

**Example 1**

```SQL
CREATE TEMPORARY FUNCTION KRingDistances AS 'com.dot.h3.hive.udf.KRingDistances';
SELECT KRingDistances(631243922056054783, 9) AS wkt;
+----------------------------------------------------+
|                        wkt                         |
+----------------------------------------------------+
| MULTIPOLYGON(((-73.90074702414034 40.86016857340853))
+----------------------------------------------------+
```

**Example 2**

```SQL
CREATE TEMPORARY FUNCTION KRingDistances AS 'com.dot.h3.hive.udf.KRingDistances';
SELECT KRingDistances('8c2a100acc687ff', 9) AS wkt;"
+----------------------------------------------------+
|                        wkt                         |
+----------------------------------------------------+
| MULTIPOLYGON(((-73.90074702414034 40.86016857340853))
+----------------------------------------------------+
```


### KRingToWkt

**Example 1**

```SQL
CREATE TEMPORARY FUNCTION KRingToWkt AS 'com.dot.h3.hive.udf.KRingToWkt';"
SELECT KRingToWkt(617733123174039551, 9) AS KRingToWkt;"
+----------------------------------------------------+"
|                     kringtowkt                     |"
+----------------------------------------------------+"
| [\"POINT(-73.90212095615803 40.86061876224212)\",  |"
+----------------------------------------------------+"
```

**Example 2**

```SQL
CREATE TEMPORARY FUNCTION KRingToWkt AS 'com.dot.h3.hive.udf.KRingToWkt';"
SELECT KRingToWkt('892a100acc7ffff', 9) AS KRingToWkt;"
+----------------------------------------------------+"
|                     kringtowkt                     |"
+----------------------------------------------------+"
| [\"POINT(-73.90212095615803 40.86061876224212)\",  |"
+----------------------------------------------------+"
```



### LatLongH3ToGeoBoundaryWkt

**Example**

```SQL
SELECT _FUNC_(40.86016, -73.90071, 12);
POLYGON((-73.90218697935661 40.862381901482266,-73.9042969767565 40.86144407471913,-73.90423087546569 40.85968095579108,-73.90205493792557 40.858855661723865,-73.89994501590182 40.85979341878112,-73.90001095604163 40.86155653960862))
--The resolution can be between 0 and 15, 15 is the most granular
```



### NumHexagons

**Example**

```SQL
CREATE TEMPORARY FUNCTION NumHexagons as 'com.dot.h3.hive.udf.NumHexagons';
SELECT NumHexagons(9) AS num_hexagons;
+---------------+
| num_hexagons  |
+---------------+
| 4842432842    |
+---------------+
```



### PolyfillToArrayH3Index

**Example**

```SQL
CREATE TEMPORARY FUNCTION PolyfillToArrayH3Index AS 'com.dot.h3.hive.udf.PolyfillToArrayH3Index';
SELECT _FUNC_('POLYGON((-71.23094863399959 42.35171702149799,-71.20507841890782 42.39384377360396,-71.18534241583312 42.40583588152941,-71.13489748711537 42.40374196572458,-71.12786523200806 42.3537116038451,-71.23094863399959 42.35171702149799))', null, 9) AS WKT;
--Returns Array<String> 
--Can take either NULL, MULTIPOLYGON or POLYGON WKT for the holes_poly_multipoly argument.
--The resolution can be between 0 and 15, 15 is the most granular
```



### PolyfillToArrayWkt

**Example**

```SQL
CREATE TEMPORARY FUNCTION PolyfillToArrayWkt AS 'com.dot.h3.hive.udf.PolyfillToArrayWkt';
SELECT PolyfillToArrayWkt('POLYGON((-71.23094863399959 42.35171702149799,-71.20507841890782 42.39384377360396,-71.18534241583312 42.40583588152941,-71.13489748711537 42.40374196572458,-71.12786523200806 42.3537116038451,-71.23094863399959 42.35171702149799))', null, 9) AS WKT;
--Returns Array<String> 
--Can take either NULL, MULTIPOLYGON or POLYGON WKT for the holes_poly_multipoly argument.
--The resolution can be between 0 and 15, 15 is the most granular
```





