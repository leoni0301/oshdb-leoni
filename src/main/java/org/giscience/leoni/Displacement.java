package org.giscience.yajie;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import org.geotools.geometry.iso.util.interpolation.ITP_Interpolation;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.OSHDBTimestampAndIndex;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampList;
import org.opengis.geometry.coordinate.GeometryFactory;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;

// Source: Assessing Crowdsourced POI Quality; Quality analysis of the Parisian OSM toponyms evolution
// Idea: the overall displacement (in meters) per feature type

public class Displacement {
    public static void main(String[] args) throws Exception {
        // database
        OSHDBJdbc oshdb = (new OSHDBH2("./baden-wuerttemberg.oshdb")).multithreading(true);
        OSHDBJdbc keytables = new OSHDBH2("./keytables");

        final String POI_KEY = "place";
        final String POI_VALUE = "village";

        TagInterpreter defaultTagInterpreter = DefaultTagInterpreter.fromJDBC(keytables.getConnection());
        // query
        SortedMap<OSHDBTimestamp, Double> result = OSMContributionView.on(oshdb)
                .keytables(keytables)
                //.areaOfInterest(new BoundingBox(7.72,47.04,10.52,49.71))
                .areaOfInterest(new OSHDBBoundingBox(8.628,49.371,8.742,49.449))
                //.areaOfInterest(new BoundingBox(80,26.3,88.5,30.8))
                .timestamps(new OSHDBTimestamps(
                        "2009-01-01",
                        "2017-01-01",
                        OSHDBTimestamps.Interval.MONTHLY)
                )
                .osmTypes(OSMType.NODE)
                .where(POI_KEY, POI_VALUE)
                .filter(contribution -> contribution.getContributionTypes().contains(ContributionType.GEOMETRY_CHANGE))
                .map(contribution -> {
                    Coordinate before = contribution.getGeometryBefore().getCentroid().getCoordinate();
                    Coordinate after = contribution.getGeometryAfter().getCentroid().getCoordinate();
                    com.vividsolutions.jts.geom.GeometryFactory gf = new com.vividsolutions.jts.geom.GeometryFactory();
                    return Geo.lengthOf(gf.createLineString(new Coordinate[] { before, after }));

                })
                .aggregateByTimestamp()
                .average();
                //.aggregateByTimestamp()
                //.sum(contribution -> {
                //    Coordinate before = contribution.getGeometryBefore().getCentroid().getCoordinate();
                //    Coordinate after = contribution.getGeometryAfter().getCentroid().getCoordinate();
                //    com.vividsolutions.jts.geom.GeometryFactory gf = new com.vividsolutions.jts.geom.GeometryFactory();
                //    return Geo.lengthOf(gf.createLineString(new Coordinate[] { before, after }));
//
                //});

        // output
        for (Map.Entry<OSHDBTimestamp, Double> entry : result.entrySet()) {
            System.out.format("%s\t%.2f\n",
                    entry.getKey().toDate(),
                    entry.getValue());
        }
    }
}
