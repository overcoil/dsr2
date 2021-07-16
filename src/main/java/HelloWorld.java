// <project-root>/src/main/java/HelloWorld.java
// from  https://github.com/delta-io/connectors/wiki/Delta-Standalone-Reader

package io.delta.standalone;


import com.google.common.collect.Iterators;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StructType;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

public class HelloWorld {

    public static void printSnapshotDetails(String title, Snapshot snapshot) {
        System.out.println("===== " + title + " =====");
        System.out.println("version: " + snapshot.getVersion());
        System.out.println("number data files: " + snapshot.getAllFiles().size());

        // enumerate all constituent files if the list is short; otherwise, show the first and last 3
        System.out.println("data files:");
        int nFile = snapshot.getAllFiles().size();
        if (  nFile < 11 ) {
            snapshot.getAllFiles().forEach(file -> System.out.println("\t" + file.getPath()));
        }
        else {
            for (int i = 0; i < 3; i++) {
                System.out.println("\t" + snapshot.getAllFiles().get(i).getPath());
            }
            System.out.println("\t...");
            for (int i = nFile - 3; i < nFile; i++) {
                System.out.println("\t" + snapshot.getAllFiles().get(i).getPath());
            }
        }

        CloseableIterator<RowRecord> iter = snapshot.open();

        if (iter.hasNext()) {
            RowRecord row = iter.next();

            StructType  st = row.getSchema();

//            Arrays.stream(st.getFields()).findFirst().filter(col -> )

            System.out.println("data schema:");
            System.out.println(row.getSchema().getTreeString());
            System.out.println("\n");

            // this is not good... but i'm lazy
            System.out.println("slow counting rows...");
            int numRows = Iterators.size(iter);

            System.out.println("\nnumber rows: " + numRows);

        }

    }

    public static void traverseDir(File dir) {
        File[] files = dir.listFiles();
        if(files != null) {
            for (final File file : files) {
                traverseDir(file);
            }
        }
        System.out.println(dir.getAbsoluteFile());
    }


    public static void main(String[] args) {
        /*
        try (Stream<Path> files = Files.walk(Paths.get("/Users/gkyc/deltalake/"))) {

            // traverse all files and sub-folders
            files.map(Path::toAbsolutePath)
                    .forEach(System.out::println);

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        */

        String  dtable;
        if (args.length == 0) {
            dtable = "/tmp/deltalake/boston-housing";
        }
        else
            dtable = args[0];

        DeltaLog log = DeltaLog.forTable(new Configuration(), dtable);

        System.out.println("Examining " + dtable);

        long  snapLo = 0, snapHi = 0;

        boolean  validSnapshot = false; // init to false to force the first iteration

        // find the lowest (first) valid Snapshot index
        while (!validSnapshot) {
            try {
                Snapshot  s = log.getSnapshotForVersionAsOf(snapLo);
                validSnapshot = true;
            } catch (IllegalArgumentException ex) {
                snapLo = snapLo + 1;
            }
        }

        // find the highest valid Snapshot index after snapLo
        snapHi = snapLo;
        validSnapshot = true;
        while (validSnapshot) {
            try {
                Snapshot  s = log.getSnapshotForVersionAsOf(snapHi);
                snapHi = snapHi + 1;
            } catch (IllegalArgumentException ex) {
                // subtract 1 since we overshot to get here
                snapHi = snapHi - 1;
                validSnapshot = false;
            }
        }

        System.out.printf("Available snapshots: "+ snapLo + " - " + snapHi + "\n");

        printSnapshotDetails("latest snapshot", log.snapshot());

        // show the earliest available version if there's more than one snapshot
        if (snapLo != snapHi)
            printSnapshotDetails("version " + snapLo + " snapshot", log.getSnapshotForVersionAsOf(snapLo));
    }
}
