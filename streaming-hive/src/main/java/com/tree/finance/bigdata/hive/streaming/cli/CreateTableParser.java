package com.tree.finance.bigdata.hive.streaming.cli;

import org.apache.commons.cli.*;

import java.io.PrintWriter;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/23 15:40
 */
public class CreateTableParser {

    private static String OPTION_NAME_DB = "db";
    private static String OPTION_NAME_TBL = "table";
    private static String OPTION_NAME_HELP = "help";
    private static String OPTION_INCLUDE_DIGIT_OPTION = "includeDigit";
    private String[] args;
    private CommandLine commandLine;

    public CreateTableParser(String[] args) {
        this.args = args;
    }

    public void init() throws ParseException{
        this.commandLine = parse();
    }

    private Options buildOptions() {
        Options options = new Options();

        Option dbOption = Option.builder(OPTION_NAME_DB).hasArg().argName(OPTION_NAME_DB).required()
                .desc("mysql database name").build();
        Option tblOption = Option.builder(OPTION_NAME_TBL).hasArg().argName(OPTION_NAME_TBL).required(false)
                .valueSeparator(',').desc("mysql database name(split by ,)").build();
        Option helpOption = Option.builder(OPTION_NAME_HELP).hasArg(false).required(false)
                .desc("help").build();
        Option includeDigitOption = Option.builder(OPTION_INCLUDE_DIGIT_OPTION).hasArg(false).required(false)
                .desc("ignore digit in database and table").build();

        options.addOption(dbOption);
        options.addOption(tblOption);
        options.addOption(includeDigitOption);
        options.addOption(helpOption);


        return options;
    }

    private CommandLine parse() throws ParseException{
        CommandLineParser parser = new DefaultParser();
        return parser.parse(buildOptions(), args);
    }

    public String getDb() {
        return commandLine.getOptionValue(OPTION_NAME_DB);
    }

    public String[] getTable() {
        return commandLine.getOptionValues(OPTION_NAME_TBL);
    }

    public boolean includeDigits() {
        return commandLine.hasOption(OPTION_INCLUDE_DIGIT_OPTION);
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
