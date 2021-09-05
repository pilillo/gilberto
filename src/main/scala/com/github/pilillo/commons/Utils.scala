package com.github.pilillo.commons

import com.github.pilillo.Settings
import com.github.pilillo.Settings.{ColNames, Formats}

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Try

object Utils {

    def parseTimeRange(from : String, to : String) : (Try[LocalDate], Try[LocalDate]) = {
        (Try(LocalDate.parse(from, Formats.inputDateFormatter)), Try(LocalDate.parse(to, Formats.inputDateFormatter)) )
    }

    val year = s"CAST(${ColNames.YEAR} as INT)"
    val month = s"CAST(${ColNames.MONTH} as INT)"
    val day = s"CAST(${ColNames.DAY} as INT)"

}