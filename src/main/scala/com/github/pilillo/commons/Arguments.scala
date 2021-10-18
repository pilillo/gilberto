package com.github.pilillo.commons;

case class Arguments(action : String = "",
                     source : String ="",
                     destination : String = "",
                     dateFrom : String = "",
                     dateTo : String = "",
                     repository : String = null,
                     metricSetInfo : String = "",
                     codeConfigPath : String = null,
                     partitionBy : String = null)