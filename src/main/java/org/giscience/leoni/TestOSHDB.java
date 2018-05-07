package org.giscience.yajie;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;

import java.util.Map;
import java.util.SortedMap;

public class TestOSHDB {
    public static void main(String[] args) throws Exception {
        // database
        OSHDBJdbc oshdb = (new OSHDBH2("./baden-wuerttemberg.oshdb")).multithreading(true);
        OSHDBJdbc keytables = new OSHDBH2("./keytables");

        // query!
        SortedMap<OSHDBTimestamp, Integer> result = OSMEntitySnapshotView.on(oshdb)
                .keytables(keytables)
                //.areaOfInterest(new BoundingBox(7.72,47.04,10.52,49.71))
                .areaOfInterest(new OSHDBBoundingBox(8.628,49.371,8.742,49.449))
                //.areaOfInterest(new BoundingBox(80,26.3,88.5,30.8))
                .timestamps(new OSHDBTimestamps(
                        "2009-01-01",
                        "2017-01-01",
                        OSHDBTimestamps.Interval.MONTHLY)
                )
                .osmTypes(OSMType.WAY)
                .where("amenity", "restaurant")
                //.where("maxspeed")
                .aggregateByTimestamp()
                //.aggregateBy(x -> x)
                .count();
                //.sum(snapshot -> Geo.lengthOf(snapshot.getGeometry()));

        // output
        for (Map.Entry<OSHDBTimestamp, Integer> entry : result.entrySet())
            System.out.format("%s\t%s\n", entry.getKey().toDate(), entry.getValue());
    }
}
