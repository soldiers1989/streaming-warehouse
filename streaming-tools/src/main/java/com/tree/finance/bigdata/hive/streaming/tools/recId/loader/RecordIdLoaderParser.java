package com.tree.finance.bigdata.hive.streaming.tools.recId.loader;

import org.apache.commons.cli.*;

import java.io.PrintWriter;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/6 11:24
 */
public class RecordIdLoaderParser {
    private static String OPTION_NAME_DB = "db";
    private static String OPTION_NAME_TBL = "table";
    private static String OPTION_CORES = "cores";
    private static String OPTION_NAME_HELP = "help";
    private static String OPTION_PARTITION_FILER = "parFilter";
    private static String OPTION_CACHE_RECORDS = "cacheRecords";
    private String[] args;
    private CommandLine commandLine;

    public RecordIdLoaderParser(String[] args) {
        this.args = args;
    }

    public void init() throws ParseException {
        this.commandLine = parse();
    }

    private Options buildOptions() {
        Options options = new Options();

        Option dbOption = Option.builder(OPTION_NAME_DB).hasArg().argName(OPTION_NAME_DB).required(true)
                .desc("mysql database name").build();
        Option tblOption = Option.builder(OPTION_NAME_TBL).hasArg().argName(OPTION_NAME_TBL).required(false)
                .valueSeparator(',').desc("mysql table name(split by ,)").build();
        Option cores = Option.builder(OPTION_CORES).hasArg().argName(OPTION_CORES).required(true)
                .valueSeparator(',').desc("mysql database name(split by ,)").build();
        Option helpOption = Option.builder(OPTION_NAME_HELP).hasArg(false).required(false)
                .desc("help").build();
        Option cacheOption = Option.builder(OPTION_CACHE_RECORDS).hasArg(true).required(false)
                .desc("cache records").build();
        Option parOption = Option.builder(OPTION_PARTITION_FILER).hasArg(true).required(false)
                .desc("partition filter like: p_y=2018 and p_m=8").build();

        options.addOption(dbOption);
        options.addOption(tblOption);
        options.addOption(cores);
        options.addOption(parOption);
        options.addOption(cacheOption);
        options.addOption(helpOption);

        return options;
    }

    private CommandLine parse() throws ParseException{
        CommandLineParser parser = new DefaultParser();
        return parser.parse(buildOptions(), args);
    }

    public boolean dbSpecified() {
        return commandLine.hasOption(OPTION_NAME_DB);
    }

    public String getDb() {
        return commandLine.getOptionValue(OPTION_NAME_DB);
    }

    public String getTable() {
        return commandLine.getOptionValue(OPTION_NAME_TBL);
    }

    public boolean isHelp() {
        return commandLine.hasOption(OPTION_NAME_HELP);
    }

    public int getCores() {
        return Integer.valueOf(commandLine.getOptionValue(OPTION_CORES));
    }

    public String getOptionPartitionFiler() {
        return commandLine.getOptionValue(OPTION_PARTITION_FILER);
    }

    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter writer = new PrintWriter(System.out, true);
        formatter.printUsage(writer, 500, "create hive table form mysql table definition", buildOptions());
        writer.flush();
        writer.close();
    }

    public Long getCacheRecords() {
        if (commandLine.hasOption(OPTION_CACHE_RECORDS)) {
            return Long.valueOf(commandLine.getOptionValue(OPTION_CACHE_RECORDS));
        } else {
            return null;
        }
    }
}
