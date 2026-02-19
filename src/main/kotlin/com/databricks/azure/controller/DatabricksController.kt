package com.databricks.azure.controller

import com.databricks.azure.models.JobInfo
import com.databricks.azure.models.ValidacoesRequest
import com.databricks.azure.service.DatabricksService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.responses.ApiResponse
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/databricks")
class DatabricksController(
    private val databricksService: DatabricksService,
) {
    // ==================================================================================//
    @Operation(
        summary = "Enviar dados para Databricks",
        description = "Envia uma lista de dados de validação para a Delta Table no Databricks."
    )
    @ApiResponse(
        responseCode = "200",
        description = "Dados enviados com sucesso"
    )
    @ApiResponse(
        responseCode = "500",
        description = "Erro interno do servidor"
    )
    @PostMapping("/enviar")
    fun enviarParaDatabricks(@RequestBody dados: List<ValidacoesRequest>): ResponseEntity<String> {
        return try {
            databricksService.enviarDados(dados)
            ResponseEntity.ok(
                "Dados enviados com sucesso para a Delta Table!"
            )
        } catch (e: Exception) {
            e.printStackTrace()
            ResponseEntity.status(500).body(
                "Erro ao enviar para o Databricks: ${e.message}"
            )
        }
    }
    // ==================================================================================//
    @Operation(
        summary = "Consultar Jobs Existentes",
        description = "Consulta os jobs existentes no Databricks e retorna informações detalhadas."
    )
    @ApiResponse(
        responseCode = "200",
        description = "Consulta realizada com sucesso"
    )
    @ApiResponse(
        responseCode = "500",
        description = "Erro interno do servidor"
    )
    @GetMapping("/jobs")
    fun consultarJobs(): ResponseEntity<List<JobInfo>> {
        return try {
            val jobs = databricksService.consultarJobs()
            ResponseEntity.ok(jobs)
        } catch (e: Exception) {
            e.printStackTrace()
            ResponseEntity.status(500).body(emptyList())
        }
    }
    // ==================================================================================//
    @Operation(
        summary = "Consultas Delta Table",
        description = "Consulta os dados da Delta Table no Databricks e retorna os resultados."
    )
    @ApiResponse(
        description = "Dados consultados com sucesso",
        responseCode = "200",
    )
    @ApiResponse(
        description = "Erro interno do servidor",
        responseCode = "500",
    )
    @GetMapping("/delta-table")
    fun consultarDeltaTable(
        @RequestParam(required = true) schema: String,
        @RequestParam(required = true) tabela: String,
        @RequestParam(required = true) limite: Int,
    ): ResponseEntity<List<Map<String, Any>>> {
        return try {
            val resultados = databricksService.consultarDeltaTable(
                schema,
                tabela,
                limite
            )
            ResponseEntity.ok(resultados)
        } catch (e: Exception) {
            e.printStackTrace()
            ResponseEntity.status(500).body(emptyList())
        }
    }
}
