package com.databricks.azure.service

import com.databricks.azure.models.ValidacoesRequest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import java.time.LocalDateTime

@Service
class DatabricksService(
    private val jdbcTemplate: JdbcTemplate
) {
    fun enviarDados(dados: List<ValidacoesRequest>) {
        val sql =
            "INSERT INTO default.validacoes_temp (" +
                    "descricao," +
                    "cliente," +
                    "competencia," +
                    "data_envio" +
                    ") VALUES (" +
                    "?," +
                    "?," +
                    "?," +
                    "?" +
                    ")"
        val agora = LocalDateTime.now().toString()
        jdbcTemplate.batchUpdate(sql, dados.map { item ->
            arrayOf(
                item.descricao,
                item.cliente,
                item.competencia,
                agora
            )
        })
    }
}
