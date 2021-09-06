package com.github.pilillo.commons;

object TimeInterval {
    val parser = new scopt.OptionParser[TimeIntervalArguments](getClass.getName){

        opt[String]('a', "action")
          .required()
          .valueName("action")
          .action((x, c) => c.copy(action = x))
          .text("Action is the pipeline to be run")

        opt[String]('s', "source")
            .required()
            .valueName("source")
            .action((x, c) => c.copy(source = x))
            .text("Source defines where to load data from")

        opt[String]('d', "destination")
            .required()
            .valueName("destination")
            .action((x, c) => c.copy(destination = x))
            .text("Destination defines where to save results")
        
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

        opt[String]('r', "repository")
          .optional()
          .valueName("target")
          .action((x, c) => c.copy(repository = x))
          .text("Target folder or endpoint of the repository")

        opt[String]('c', "code-config-path")
          .optional()
          .valueName("path")
          .action((x, c) => c.copy(codeConfigPath = x))
          .text("Path of the file containing the checks to instruct the validator")

        opt[String]('p', "partition-by")
          .optional()
          .valueName("columns")
          .action((x, c) => c.copy(partitionBy = x))
          .text("Columns to use to partition the resulting dataframe")
    }
    def parse(args : Array[String]) : Option[TimeIntervalArguments] = parser.parse(args, TimeIntervalArguments())
}