package com.databricks.azure.controller

import com.databricks.azure.models.JobInfo
import com.databricks.azure.models.ValidacoesRequest
import com.databricks.azure.service.DatabricksService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.responses.ApiResponse
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

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
            databricksService.sendDados(dados)
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
            val jobs = databricksService.getJobs()
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
            val resultados = databricksService.getDeltaTable(
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
    // ==================================================================================//
    @Operation(
        summary = "Consultar Todas Delta Tables",
        description = "Consulta os nomes de todas as Delta Tables disponíveis no Databricks."
    )
    @ApiResponse(
        description = "Consulta realizada com sucesso",
        responseCode = "200",
    )
    @ApiResponse(
        description = "Erro interno do servidor",
        responseCode = "500",
    )
    @GetMapping("/delta-tables")
    fun consultarTodasDeltaTables(): ResponseEntity<List<String>> {
        return try {
            val tables = databricksService.getAllDeltaTables()
            ResponseEntity.ok(tables)
        } catch (e: Exception) {
            e.printStackTrace()
            ResponseEntity.status(500).body(emptyList())
        }
    }
    // ==================================================================================//
    @Operation(
        summary = "Rodar Jobs Automaticamente",
        description = "Roda os jobs automaticamente no Databricks, seguindo a lógica definida no serviço."
    )
    @ApiResponse(
        description = "Jobs rodados automaticamente com sucesso",
        responseCode = "200",
    )
    @ApiResponse(
        description = "Erro ao rodar os jobs automaticamente",
        responseCode = "400",
    )
    fun runJobsAutomaticamente(): ResponseEntity<String> {
         return try {
             databricksService.runJobsAutomaticamente()
             return ResponseEntity.ok("Jobs automaticamente!")
        } catch (e: Exception) {
            e.printStackTrace()
            ResponseEntity.status(400).body("Erro ao rodar os jobs automaticamente: ${e.message}")
        }
    }
    // ==================================================================================//
}
