package com.github.pilillo.commons;

case class TimeIntervalArguments(action : String = "",
                                 source : String ="",
                                 dateFrom : String = "",
                                 dateTo : String = "",
                                 destination : String = "",
                                 repository : String = null,
                                 codeConfigPath : String = null,
                                 partitionBy : String = null
                                )