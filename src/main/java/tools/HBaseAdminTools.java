package tools;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.NavigableMap;

public class HBaseAdminTools {

    private static Configuration conf = HBaseConfiguration.create();

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException,
            InterruptedException {
        Options options = constructOptions();
        if (args.length < 1) {
            printUsage(options, System.out);
            return;
        }
        CommandLineParser parser = new PosixParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("closeregions")) {
                String table = cmd.getOptionValue("closeregions");
                closeRegionsInTable(table);
            } else if (cmd.hasOption("deletemeta")) {
                String table = cmd.getOptionValue("deletemeta");
                deleteFromMeta(table);
            } else if (cmd.hasOption("scanmeta")) {
                scanMeta();
            } else {
                printUsage(options, System.out);
            }
        } catch (ParseException e) {
            printUsage(options, System.out);
        }

    }

    /**
     * Delete table's entries from .META.
     *
     * @param tableName table name
     * @throws IOException
     */
    private static void deleteFromMeta(String tableName) throws IOException {
        HTable table = null;
        HTable metaTable = null;
        try {
            table = new HTable(conf, tableName);
            metaTable = new HTable(conf, ".META.");
            HBaseAdmin admin = new HBaseAdmin(conf);
            boolean exists = admin.tableExists(tableName);
            if (exists) {
                System.out.println(String.format("table %s exists", tableName));
            } else {
                System.out.println(String.format(
                        "table %s doesn't exist; returning", tableName));
                return;
            }

            NavigableMap<HRegionInfo, ServerName> regions = table
                    .getRegionLocations();
            System.out
                    .println("This will delete the following regions from .META.: ");
            for (HRegionInfo region : regions.keySet()) {
                System.out.println("    " + region.getRegionNameAsString());
            }
            System.out
                    .println("Are you sure you want to proceed? [y/n(default)]");

            BufferedReader bufferRead = new BufferedReader(
                    new InputStreamReader(System.in));
            String response = bufferRead.readLine();

            if (response.equalsIgnoreCase("y")) {
                for (HRegionInfo region : regions.keySet()) {
                    Delete d = new Delete(Bytes.toBytes(region
                            .getRegionNameAsString()));
                    metaTable.delete(d);
                }
                System.out.println("finished deleting regions from .META.");
            } else {
                System.out
                        .println("returning without deleting regions from .META.");
            }
        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            if (table != null) {
                table.close();
            }
            if (metaTable != null) {
                metaTable.close();
            }
        }

    }

    /**
     * Close regions in table.
     *
     * @param tableName table name
     * @throws IOException
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     */
    private static void closeRegionsInTable(String tableName)
            throws IOException, MasterNotRunningException,
            ZooKeeperConnectionException {
        HTable table = null;
        try {
            table = new HTable(conf, tableName);
            HBaseAdmin admin = new HBaseAdmin(conf);
            boolean exists = admin.tableExists(tableName);
            if (exists) {
                System.out.println(String.format("table %s exists", tableName));
            } else {
                System.out.println(String.format(
                        "table %s doesn't exist; returning", tableName));
                return;
            }
            NavigableMap<HRegionInfo, ServerName> regions = table
                    .getRegionLocations();
            System.out.println("This will close the following regions: ");
            for (HRegionInfo region : regions.keySet()) {
                System.out.println("    " + region.getRegionNameAsString());
            }
            System.out
                    .println("Are you sure you want to proceed? [y/n(default)]");

            BufferedReader bufferRead = new BufferedReader(
                    new InputStreamReader(System.in));
            String response = bufferRead.readLine();
            if (response.equalsIgnoreCase("y")) {
                for (HRegionInfo region : regions.keySet()) {
                    admin.closeRegion(region.getRegionNameAsString(), null);
                }
                System.out.println("finished closing regions");
            } else {
                System.out.println("returning without closing regions");
            }
        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * Scans .META.
     *
     * @throws IOException
     */
    private static void scanMeta() throws IOException {
        System.out.println("Scanning .META.");
        HTable metaTable = new HTable(conf, ".META.");
        byte[] family = Bytes.toBytes("info");
        ResultScanner scanner = metaTable.getScanner(family);

        for (Result result : scanner) {
            System.out.println("Found row: "
                    + Bytes.toString(result.getRow()));
        }

        scanner.close();
    }

    /**
     * Construct command line options.
     *
     * @return command line options
     */
    private static Options constructOptions() {
        Options options = new Options();
        Option closeRegionsOption = OptionBuilder.withArgName("table").hasArg()
                .withDescription("close regions in table")
                .create("closeregions");
        options.addOption(closeRegionsOption);
        Option deleteFromMetaOption = OptionBuilder.withArgName("table")
                .hasArg().withDescription("delete table entries from .META.")
                .create("deletemeta");
        options.addOption(deleteFromMetaOption);
        Option scanMetaOption = OptionBuilder.withDescription("scan .META.")
                .create("scanmeta");
        options.addOption(scanMetaOption);
        return options;
    }

    /**
     * Print usage information to provided OutputStream.
     *
     * @param options Command-line options to be part of usage.
     * @param out     OutputStream to which to write the usage information.
     */
    public static void printUsage(final Options options, final OutputStream out) {
        final String applicationName = "java -cp hbaseadmin.jar tools.HBaseAdminTools";
        final HelpFormatter usageFormatter = new HelpFormatter();
        usageFormatter.printHelp(applicationName, options);
    }

}
