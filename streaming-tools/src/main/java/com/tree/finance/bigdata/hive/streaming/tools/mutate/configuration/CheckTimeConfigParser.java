package com.tree.finance.bigdata.hive.streaming.tools.mutate.configuration;

import org.apache.commons.cli.*;

import java.io.PrintWriter;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/12 13:51
 */
public class CheckTimeConfigParser {
    private static String OPTION_NAME_DB = "db";
    private static String OPTION_NAME_TBL = "table";
    private static String OPTION_PARTITION = "par";
    private static String TIME_MILLIS = "time_millis";
    private static String OPTION_NAME_HELP = "help";
    private String[] args;
    private CommandLine commandLine;

    public CheckTimeConfigParser(String[] args) {
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
        Option time = Option.builder(TIME_MILLIS).hasArg().argName(TIME_MILLIS).required(true)
                .valueSeparator(',').desc("update time in milli sec").build();
        Option parOption = Option.builder(OPTION_PARTITION).hasArg().argName(OPTION_PARTITION).required(false)
                .valueSeparator(',').desc("partition name eg. p_y=2018/p_m=1/p_d=1").build();


        Option helpOption = Option.builder(OPTION_NAME_HELP).hasArg(false).required(false)
                .desc("help").build();

        options.addOption(dbOption);
        options.addOption(tblOption);
        options.addOption(parOption);
        options.addOption(time);
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

    public String getTable() {
        return commandLine.getOptionValue(OPTION_NAME_TBL);
    }

    public boolean isHelp() {
        return commandLine.hasOption(OPTION_NAME_HELP);
    }

    public Long getTimeMillis() {
        return Long.valueOf(commandLine.getOptionValue(TIME_MILLIS));
    }

    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter writer = new PrintWriter(System.out, true);
        formatter.printUsage(writer, 500, "create hive table form mysql table definition", buildOptions());
        writer.flush();
        writer.close();
    }
}
