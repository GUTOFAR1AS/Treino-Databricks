package com.databricks.azure.utils

import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

object DateUtils {
    fun formatarHorarioBrasilia(horarioBrasilia: LocalDateTime?): String {
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val zonaBrasilia = ZoneId.of("America/Sao_Paulo")
        val horarioLocal = horarioBrasilia?.atZone(zonaBrasilia)?.toLocalDateTime()
        return horarioLocal?.format(formatter) ?: ""
    }
}