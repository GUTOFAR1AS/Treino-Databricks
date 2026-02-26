package com.databricks.azure.service.medallion

import com.databricks.azure.models.ModelBronze
import com.databricks.azure.models.ModelGold
import com.databricks.azure.models.ModelSilver
import com.databricks.azure.utils.DateUtils.formatarHorarioBrasilia
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.UUID

@Service
class MedallionService : IMedallion  {
    override fun processBronze(fileContent: String): List<ModelBronze> {
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

    // SILVER: Limpeza e aplicação de regras
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

    // GOLD: Agregação e transformação final
    override fun processGold(silverData: List<ModelSilver>): List<ModelGold> {
        val agora = LocalDateTime.now()
        return silverData
            .filter { it.validationErrors!!.isEmpty() }
            .map { silver ->
                ModelGold(
                    id = silver.id,
                    finalData = silver.cleanedData,
                    aggregatedDate = formatarHorarioBrasilia(agora),
                    businessRules = mapOf(
                        "length" to silver.cleanedData!!.length,
                        "hasNumbers" to silver.cleanedData.any { it.isDigit() }
                    )
                )
            }
    }
    private fun validateBasic(line: String): Boolean {
        return line.isNotBlank() && line.length > 2
    }
}