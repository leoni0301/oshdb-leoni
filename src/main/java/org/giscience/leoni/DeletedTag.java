package org.giscience.yajie;

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

import java.util.Map;
import java.util.SortedMap;
// Source: The annotation process in OSM
// Idea: count the number of tag change

public class DeletedTag {
    public static void main(String[] args) throws Exception {
        // database
        OSHDBJdbc oshdb = (new OSHDBH2("./baden-wuerttemberg.oshdb")).multithreading(true);
        OSHDBJdbc keytables = new OSHDBH2("./keytables");

        // query
        SortedMap<OSHDBTimestampAndIndex<String>, Integer> result = OSMContributionView.on(oshdb)
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
                .map(snapshot -> {
                    // write analysis code here
                    try {
                        if (snapshot.getContributionTypes().contains(ContributionType.DELETION)) {
                            //System.err.println("found tag deleted");
                            //System.err.println(snapshot.getEntityBefore().getId());
                            //System.err.println(snapshot.getEntityAfter().getTags().toString());
                            //System.err.println(snapshot.getTimestamp().toDate());
                            return "deleted";
                        }
                    } catch (Exception e) {
                        return "oshdb-bug";
                    }
                    return "no change";
                })
                .aggregateByTimestamp()
                .aggregateBy(x -> x)
                .count();

        // output
        for (Map.Entry<OSHDBTimestampAndIndex<String>, Integer> entry : result.entrySet()) {
            System.out.format("%s\t%s\t%d%n",
                    entry.getKey().getTimeIndex().toDate(),
                    entry.getKey().getOtherIndex(),
                    entry.getValue());
        }
    }
}
