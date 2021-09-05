package com.github.pilillo.commons;

object TimeInterval {
    val parser = new scopt.OptionParser[TimeIntervalArguments](getClass.getName){
        opt[String]('s', "source")
            .required()
            .valueName("source")
            .action((x, c) => c.copy(source = x))
            .text("Source")

        opt[String]('d', "destination")
            .required()
            .valueName("destination")
            .action((x, c) => c.copy(destination = x))
            .text("Destination")
        
        opt[String]('f', "from")
            .required()
            .valueName("date")
            .action((x, c) => c.copy(dateFrom = x))
            .text("Beginning")

        opt[String]('t', "to")
            .required()
            .valueName("date")
            .action((x, c) => c.copy(dateTo = x))
            .text("End")
    }
    def parse(args : Array[String]) : Option[TimeIntervalArguments] = parser.parse(args, TimeIntervalArguments())
}