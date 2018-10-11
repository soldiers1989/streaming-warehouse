package com.tree.finance.bigdata.hive.streaming.tools.avro;

import org.apache.commons.cli.*;

import java.io.PrintWriter;

public class AvroCatParser {

    private static String OPTION_NAME_PATH = "path";
    private static String OPTION_NAME_HELP = "help";
    private String[] args;
    private CommandLine commandLine;

    public AvroCatParser(String[] args) {
        this.args = args;
    }

    public void init() throws ParseException {
        this.commandLine = parse();
    }

    private Options buildOptions() {
        Options options = new Options();

        Option pathOption = Option.builder(OPTION_NAME_PATH).hasArg().argName(OPTION_NAME_PATH).required(true)
                .desc("avro path dir or file").build();
        Option help = Option.builder(OPTION_NAME_HELP).hasArg(false).argName(OPTION_NAME_HELP).required(false)
                .desc("help desc").build();

        options.addOption(pathOption);
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
        formatter.printUsage(writer, 500, "cat avro directory or files", buildOptions());
        writer.flush();
        writer.close();
    }

    public String[] getPath() {
        return commandLine.getOptionValues(OPTION_NAME_PATH);
    }
}
