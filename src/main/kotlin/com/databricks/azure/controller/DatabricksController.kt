package com.databricks.azure.controller

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
}