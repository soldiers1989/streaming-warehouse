package com.tree.finance.bigdata.hive.streaming.tools.recId.insight;

import org.apache.commons.cli.*;

import java.io.PrintWriter;

public class InsightParser {
    private static String OPTION_NAME_ID = "ids";
    private static String OPTION_NAME_TABLE = "table";
    private static String OPTION_NAME_HELP = "help";
    private String[] args;
    private CommandLine commandLine;

    public InsightParser(String[] args) {
        this.args = args;
    }

    public void init() throws ParseException {
        this.commandLine = parse();
    }

    private Options buildOptions() {
        Options options = new Options();

        Option idOption = Option.builder(OPTION_NAME_ID).hasArg().argName(OPTION_NAME_ID).required(true)
                .desc("BuizIds(no hash prefix), split by , ").build();
        Option tableOption = Option.builder(OPTION_NAME_TABLE).hasArg().argName(OPTION_NAME_TABLE).required(true)
                .desc("hive table name").build();
        Option help = Option.builder(OPTION_NAME_HELP).hasArg(false).argName(OPTION_NAME_HELP).required(false)
                .desc("help desc").build();

        options.addOption(idOption);
        options.addOption(tableOption);
        options.addOption(help);

        return options;
    }

    private CommandLine parse() throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(buildOptions(), args);
    }

    public boolean isHelp() {
        return commandLine.hasOption(OPTION_NAME_HELP);
    }

    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter writer = new PrintWriter(System.out, true);
        formatter.printUsage(writer, 500, "cat recrdId in HBase", buildOptions());
        writer.flush();
        writer.close();
    }

    public String[] getIds() {
        return commandLine.getOptionValues(OPTION_NAME_ID);
    }

    public String getTable() {
        return commandLine.getOptionValue(OPTION_NAME_TABLE);
    }
}
