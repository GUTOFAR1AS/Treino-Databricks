package com.databricks.azure.service

import com.databricks.azure.models.JobInfo
import com.databricks.azure.models.ValidacoesRequest
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import java.time.LocalDateTime

@Service
class DatabricksService(
    private val jdbcTemplate: JdbcTemplate,
    private val restTemplate: RestTemplate,
    @Value("\${databricks.host}") private val databricksHost: String,
    @Value("\${databricks.token}") private val databricksToken: String
) : DatabricksImpl {
    override fun enviarDados(dados: List<ValidacoesRequest>) {
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

    override fun consultarJobs(): List<JobInfo> {
        val url = "$databricksHost/api/2.1/jobs/list"
        val headers = HttpHeaders().apply {
            set("Authorization", "Bearer $databricksToken")
        }

        val response = restTemplate.exchange(
            url,
            HttpMethod.GET,
            HttpEntity<Any>(headers),
            object : ParameterizedTypeReference<Map<String, Any>>() {}
        )

        val jobs = response.body?.get("jobs") as? List<Map<String, Any>> ?: emptyList()
        return jobs.mapNotNull { job ->
            val settings = job["settings"] as? Map<String, Any> ?: return@mapNotNull null
            val nome = settings["name"] as? String ?: "Sem nome"
            val criador = job["creator_user_name"] as? String ?: "Desconhecido"

            val tasks = settings["tasks"] as? List<Map<String, Any>>
            val notebookTask = tasks?.firstOrNull()?.get("notebook_task") as? Map<String, Any>
            val notebookPath = notebookTask?.get("notebook_path") as? String

            val configuracoes = mutableMapOf<String, Any>()
            settings["schedule"]?.let { configuracoes["schedule"] = it }
            settings["max_concurrent_runs"]?.let { configuracoes["max_concurrent_runs"] = it }
            settings["timeout_seconds"]?.let { configuracoes["timeout_seconds"] = it }

            JobInfo(
                nome = nome,
                criador = criador,
                notebookPath = notebookPath,
                temConfiguracoes = configuracoes.isNotEmpty(),
                configuracoes = configuracoes.ifEmpty { null }
            )
        }
    }

    override fun consultarDeltaTable(schema: String, tabela: String, limite: Int): List<Map<String, Any>> {
        val sql = "SELECT * FROM $schema.$tabela LIMIT $limite"
        return jdbcTemplate.queryForList(sql) as List<Map<String, Any>>
    }
}
