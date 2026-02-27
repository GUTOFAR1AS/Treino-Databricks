package com.databricks.azure.service.medallion

import com.databricks.azure.models.ModelBronze
import com.databricks.azure.models.ModelGold
import com.databricks.azure.models.ModelSilver
import com.databricks.azure.utils.DateUtils.formatarHorarioBrasilia
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.UUID

@Service
class MedallionService(
    private val jdbcTemplate: JdbcTemplate
) : IMedallion {

    override fun processBronze(fileContent: String, fileName: String): List<ModelBronze> {
        val agora = LocalDateTime.now()
        return fileContent.lines()
            .filter { it.isNotBlank() }
            .map { line ->
                ModelBronze(
                    id = UUID.randomUUID().toString(),
                    rawData = line,
                    loadDate = formatarHorarioBrasilia(agora),
                    isValid = validateBasic(line)
                )
            }
    }

    override fun processSilver(bronzeData: List<ModelBronze>): List<ModelSilver> {
        return bronzeData
            .filter { it.isValid }
            .map { bronze ->
                val errors = mutableListOf<String>()
                val cleaned = bronze.rawData.trim().uppercase()
                val agora = LocalDateTime.now()

                if (cleaned.length < 3) errors.add("Dados muito curtos")
                if (!cleaned.contains(Regex("[A-Z0-9]"))) errors.add("Formato inválido")

                ModelSilver(
                    id = bronze.id,
                    cleanedData = cleaned,
                    processDate = formatarHorarioBrasilia(agora),
                    validationErrors = errors
                )
            }
    }

    override fun processGold(silverData: List<ModelSilver>, fileName: String): List<ModelGold> {
        val agora = LocalDateTime.now()
        return silverData
            .filter { it.validationErrors?.isEmpty() ?: true }
            .map { silver ->
                ModelGold(
                    idArquivo = silver.id,
                    finalData = silver.cleanedData,
                    nomeArquivo = fileName,
                    aggregatedDate = formatarHorarioBrasilia(agora),
                    businessRules = mapOf(
                        "length" to (silver.cleanedData?.length ?: 0),
                        "hasNumbers" to (silver.cleanedData?.any { it.isDigit() } ?: false)
                    )
                )
            }
    }

    override fun saveGold(idArquivo: String, nomeArquivo: String): List<ModelGold> {
        val agora = LocalDateTime.now()
        val sql = """
            INSERT INTO gold_table (idArquivo, nomeArquivo, finalData, aggregatedDate, businessRules)
            VALUES (?, ?, ?, ?, ?)
        """.trimIndent()

        val businessRulesJson = """{}"""
        return try {
            jdbcTemplate.update(sql, idArquivo, "Dados processados e salvos", formatarHorarioBrasilia(agora), businessRulesJson)
            listOf(
                ModelGold(
                    idArquivo = idArquivo,
                    nomeArquivo = nomeArquivo,
                    finalData = "Dados processados e salvos com sucesso",
                    aggregatedDate = formatarHorarioBrasilia(agora),
                    businessRules = mapOf("status" to "Salvo com sucesso")
                )
            )
        } catch (e: Exception) {
            listOf(
                ModelGold(
                    idArquivo = idArquivo,
                    nomeArquivo = nomeArquivo,
                    finalData = "Erro ao salvar dados: ${e.message}",
                    aggregatedDate = formatarHorarioBrasilia(agora),
                    businessRules = mapOf("status" to "Erro: ${e.localizedMessage}")
                )
            )
        }
    }

    override fun saveGoldData(goldDataList: List<ModelGold>): List<ModelGold> {
        if (goldDataList.isEmpty()) return emptyList()
        val objectMapper = ObjectMapper()

        // SQL corrigido para incluir nomeArquivo
        val sql = """
        INSERT INTO gold_table (idArquivo, nomeArquivo, finalData, aggregatedDate, businessRules)
        VALUES (?, ?, ?, ?, ?)
    """.trimIndent()

        val batchArgs = goldDataList.map { gold ->
            val brJson = try {
                if (gold.businessRules != null) objectMapper.writeValueAsString(gold.businessRules) else "{}"
            } catch (_: Exception) { "{}" }

            // Adicionado gold.nomeArquivo na segunda posição
            arrayOf(
                gold.idArquivo ?: "",
                gold.nomeArquivo ?: "sem_nome",
                gold.finalData ?: "",
                gold.aggregatedDate ?: "",
                brJson
            )
        }

        return try {
            jdbcTemplate.batchUpdate(sql, batchArgs)
            goldDataList.map { it.copy(businessRules = mapOf("status" to "Inserido com sucesso")) }
        } catch (e: Exception) {
            goldDataList.map { gold -> gold.copy(finalData = "Erro: ${e.localizedMessage}") }
        }
    }

    override fun createTableGold(): ModelGold {
        val sql = """
            CREATE TABLE IF NOT EXISTS gold_table (
                idArquivo STRING,
                nomeArquivo STRING,
                finalData STRING,
                aggregatedDate TIMESTAMP,
                businessRules STRING
            )
            USING DELTA
        """.trimIndent()

        return try {
            jdbcTemplate.execute(sql)
            ModelGold(
                idArquivo = null,
                nomeArquivo = null,
                finalData = "Tabela Gold criada com sucesso",
                aggregatedDate = null,
                businessRules = null
            )
        } catch (e: Exception) {
            ModelGold(
                idArquivo = null,
                nomeArquivo = null,
                finalData = "Erro ao criar tabela Gold: ${e.message}",
                aggregatedDate = null,
                businessRules = null
            )
        }
    }

    private fun validateBasic(line: String): Boolean {
        return line.isNotBlank() && line.length > 2
    }
}
