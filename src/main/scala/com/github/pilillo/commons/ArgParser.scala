package com.github.pilillo.commons;

object ArgParser {
    val parser = new scopt.OptionParser[Arguments](getClass.getName){
        // action to run
        opt[String]('a', "action")
          .required()
          .valueName("action")
          .action((x, c) => c.copy(action = x))
          .text("Action is the pipeline to be run")

        // data source
        opt[String]('s', "source")
          .required()
          .valueName("source")
          .action((x, c) => c.copy(source = x))
          .text("Source defines where to load data from")

        // destination
        opt[String]('d', "destination")
          .required()
          .valueName("destination")
          .action((x, c) => c.copy(destination = x))
          .text("Destination defines where to save results")

        // time interval
        opt[String]('f', "from")
          .required()
          .valueName("date")
          .action((x, c) => c.copy(dateFrom = x))
          .text("Beginning of the time interval")

        opt[String]('t', "to")
          .required()
          .valueName("date")
          .action((x, c) => c.copy(dateTo = x))
          .text("End of the time interval")

        // metric repository
        opt[String]('r', "repository")
          .optional()
          .valueName("target")
          .action((x, c) => c.copy(repository = x))
          .text("Target folder or endpoint of the repository of kind http://host:port/metricstore/")

        opt[String]('n', "metricset")
          .optional()
          .valueName("name")
          .action((x, c) => c.copy(metricSetInfo = x))
          .text("MetricSet info as name:version:description to be used with a Mastro repository")

        // code config
        opt[String]('c', "code-config-path")
          .optional()
          .valueName("path")
          .action((x, c) => c.copy(codeConfigPath = x))
          .text("Path of the file containing the checks to instruct the validator")

        // partition-by for resulting dataframe
        opt[String]('p', "partition-by")
          .optional()
          .valueName("columns")
          .action((x, c) => c.copy(partitionBy = x))
          .text("Columns to use to partition the resulting dataframe")
    }
    def parse(args : Array[String]) : Option[Arguments] = parser.parse(args, Arguments())
}