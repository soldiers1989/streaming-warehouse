package com.tree.finance.bigdata.hive.streaming.tools.hive;

import org.apache.commons.cli.*;

import java.io.PrintWriter;

public class CompactTableParser {
    private static String OPTION_NAME_DB = "db";
    private static String OPTION_NAME_TBL = "table";
    private static String OPTION_PARTITION = "par";
    private static String OPTION_NAME_HELP = "help";
    private static String OPTION_COMPACT_DELTAS = "deltas";
    static final int DEFAULT_DELTAS = 5;
    private String[] args;
    private CommandLine commandLine;

    public CompactTableParser(String[] args) {
        this.args = args;
    }

    public void init() throws ParseException {
        this.commandLine = parse();
    }

    private Options buildOptions() {
        Options options = new Options();

        Option dbOption = Option.builder(OPTION_NAME_DB).hasArg().argName(OPTION_NAME_DB).required(true)
                .desc("hive database name").build();
        Option tblOption = Option.builder(OPTION_NAME_TBL).hasArg().argName(OPTION_NAME_TBL).required(false)
                .valueSeparator(',').desc("hive table name").build();
        Option parOption = Option.builder(OPTION_PARTITION).hasArg().argName(OPTION_PARTITION).required(false)
                .valueSeparator(',').desc("partition name eg. p_y=2018 and p_m=1 and p_d=1").build();
        Option deltaOption = Option.builder(OPTION_COMPACT_DELTAS).hasArg().argName(OPTION_COMPACT_DELTAS).required(false)
                .valueSeparator(',').desc("compact delta threashold, default is 5").build();
        Option helpOption = Option.builder(OPTION_NAME_HELP).hasArg(false).required(false)
                .desc("help").build();

        options.addOption(dbOption);
        options.addOption(tblOption);
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

    public String getPar() {
        return commandLine.getOptionValue(OPTION_PARTITION);
    }

    public int getDeltas() {
        return commandLine.hasOption(OPTION_COMPACT_DELTAS) ? Integer.valueOf(commandLine.getOptionValue(OPTION_COMPACT_DELTAS))
                : DEFAULT_DELTAS;
    }

    public String[] getTables() {
        return commandLine.getOptionValues(OPTION_NAME_TBL);
    }

    public boolean isHelp() {
        return commandLine.hasOption(OPTION_NAME_HELP);
    }

    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter writer = new PrintWriter(System.out, true);
        formatter.printUsage(writer, 500, "create hive table form mysql table definition", buildOptions());
        writer.flush();
        writer.close();
    }
}
