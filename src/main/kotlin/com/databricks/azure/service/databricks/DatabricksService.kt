package com.databricks.azure.service.databricks

import com.databricks.azure.models.JobInfo
import com.databricks.azure.models.ValidacoesRequest
import com.databricks.azure.utils.DateUtils
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import java.time.LocalDateTime
import kotlin.collections.set

@Service
class DatabricksService(
    private val jdbcTemplate: JdbcTemplate,
    private val restTemplate: RestTemplate,
    @Value("\${databricks.host}") private val databricksHost: String,
    @Value("\${databricks.token}") private val databricksToken: String
) : DatabricksImpl {
    override fun sendDados(dados: List<ValidacoesRequest>) {
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
        val agora = LocalDateTime.now()
        jdbcTemplate.batchUpdate(sql, dados.map { item ->
            arrayOf(
                item.descricao,
                item.cliente,
                item.competencia,
                DateUtils.formatarHorarioBrasilia(agora)
            )
        })
    }

    override fun getJobs(): List<JobInfo> {
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
                id = job["job_id"]?.toString(),
                nome = nome,
                criador = criador,
                notebookPath = notebookPath,
                temConfiguracoes = configuracoes.isNotEmpty(),
                configuracoes = configuracoes.ifEmpty { null }
            )
        }
    }

    override fun getDeltaTable(schema: String, tabela: String, limite: Int): List<Map<String, Any>> {
        val sql = "SELECT * FROM $schema.$tabela LIMIT $limite"
        return jdbcTemplate.queryForList(sql) as List<Map<String, Any>>
    }

    override fun getAllDeltaTables(): List<String> {
        val sql = "SHOW TABLES IN default"
        val tables = jdbcTemplate.queryForList(sql)
        return tables.mapNotNull { it["tableName"] as? String }
    }

    override fun startCluster(clusterId: String) {
        val url = "$databricksHost/api/2.0/clusters/start"

        val headers = HttpHeaders().apply {
            setBearerAuth(databricksToken)
            contentType = MediaType.APPLICATION_JSON
        }

        val payload = mapOf(
            "cluster_id" to clusterId
        )

        try {
            val response = restTemplate.postForEntity(
                url,
                HttpEntity(payload, headers),
                String::class.java
            )

            if (response.statusCode.is2xxSuccessful) {
                println("Cluster iniciado com sucesso.")
            } else {
                throw RuntimeException("Erro ao iniciar cluster: ${response.statusCode}")
            }

        } catch (e: Exception) {
            throw RuntimeException("Erro ao iniciar cluster: ${e.message}", e)
        }
    }

    override fun stopCluster(clusterId: String) {
        val url = "$databricksHost/api/2.0/clusters/delete"

        val headers = HttpHeaders().apply {
            setBearerAuth(databricksToken)
            contentType = MediaType.APPLICATION_JSON
        }

        val payload = mapOf(
            "cluster_id" to clusterId
        )

        try {
            val response = restTemplate.postForEntity(
                url,
                HttpEntity(payload, headers),
                String::class.java
            )

            if (response.statusCode.is2xxSuccessful) {
                println("Cluster parado com sucesso.")
            } else {
                throw RuntimeException("Erro ao parar cluster: ${response.statusCode}")
            }

        } catch (e: Exception) {
            throw RuntimeException("Erro ao parar cluster: ${e.message}", e)
        }
    }
}
