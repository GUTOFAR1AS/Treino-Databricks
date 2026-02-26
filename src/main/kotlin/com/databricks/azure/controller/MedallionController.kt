package com.databricks.azure.controller

import com.databricks.azure.models.ModelGold
import com.databricks.azure.service.medallion.IMedallion
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.multipart.MultipartFile


@RestController
@RequestMapping("/api/medallion")
class MedallionController(
    private val medallionService: IMedallion,
) {
    @Operation(
        summary = "Processa arquivo e executa as etapas Bronze, Silver e Gold",
        description = "Recebe um arquivo, processa as etapas Bronze, Silver e Gold, e retorna os dados processados."
    )
    @ApiResponses(
        ApiResponse(responseCode = "200", description = "Processamento bem-sucedido"),
        ApiResponse(responseCode = "400", description = "Requisição inválida"),
        ApiResponse(responseCode = "500", description = "Erro interno do servidor")
    )
    @PostMapping("/medallion", consumes = ["multipart/form-data"])
    @ApiResponse(responseCode = "200", description = "Processamento bem-sucedido")
    fun medallion(
        @RequestParam("file") file: MultipartFile,
    ): List<ModelGold> {
        val fileContent = file.inputStream.bufferedReader().use { it.readText() }
        val bronzeData = medallionService.processBronze(fileContent)
        val silverData = medallionService.processSilver(bronzeData)
        return medallionService.processGold(silverData)
    }
    // ===============================================================================================================//
    @Operation(
        summary = "Salvar os dados na tabela Gold.",
        description = "Recebe os dados processados e salva na tabela Gold, retornando os dados salvos."
    )
    @ApiResponses(
        ApiResponse(responseCode = "200", description = "Dados salvos com sucesso"),
        ApiResponse(responseCode = "400", description = "Requisição inválida"),
        ApiResponse(responseCode = "500", description = "Erro interno do servidor")
    )
    @PostMapping("/saveGold")
    fun saveGold(
            @RequestParam("idArquivo") idArquivo: String,
    ): List<ModelGold> {
        val retorno = medallionService.saveGold(idArquivo)
        return retorno
    }
    // ===============================================================================================================//
}
