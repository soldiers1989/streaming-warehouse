package com.tree.finance.bigdata.hive.streaming.tools.repair;

import org.apache.commons.cli.*;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

public class DuplicationRemoverParser {
    private static final String OPTION_EXECUTED = "executed";
    private static String OPTION_NAME_DB = "db";
    private static String OPTION_NAME_TBL = "table";
    private static String OPTION_CORES = "cores";
    private static String OPTION_NAME_HELP = "help";
    private static String OPTION_PARTITION_FILER = "parFilter";
    private String[] args;
    private CommandLine commandLine;

    public DuplicationRemoverParser(String[] args) {
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
                .valueSeparator(',').desc("mysql database name(split by ,)").build();
        Option cores = Option.builder(OPTION_CORES).hasArg().argName(OPTION_CORES).required(true)
                .valueSeparator(',').desc("mysql database name(split by ,)").build();
        Option helpOption = Option.builder(OPTION_NAME_HELP).hasArg(false).required(false)
                .desc("help").build();
        Option parOption = Option.builder(OPTION_PARTITION_FILER).hasArg(true).required(false)
                .desc("partition filter like: p_y=2018 and p_m=8").build();
        Option executedOption = Option.builder(OPTION_EXECUTED).hasArg(false).required(false)
                .desc("have executed repair before").build();

        options.addOption(dbOption);
        options.addOption(tblOption);
        options.addOption(cores);
        options.addOption(parOption);
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

    public List<String> getTables() {
        return Arrays.asList(commandLine.getOptionValues(OPTION_NAME_TBL));
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

    public boolean getExecuted() {
        return commandLine.hasOption(OPTION_EXECUTED);
    }
}
