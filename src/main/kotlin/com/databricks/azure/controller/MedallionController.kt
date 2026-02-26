package com.databricks.azure.controller

import com.databricks.azure.models.ModelGold
import com.databricks.azure.service.medallion.IMedallion
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.responses.ApiResponse
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.multipart.MultipartFile


@RestController
@RequestMapping("/api/medallion")
class MedallionController(
    private val medallionService: IMedallion,
) {
    @PostMapping("/medallion")
    @Operation(summary = "Processa arquivo e executa as etapas Bronze, Silver e Gold")
    @ApiResponse(responseCode = "200", description = "Processamento bem-sucedido")
    fun medallion(
        @RequestParam("file") file: MultipartFile
    ): List<ModelGold> {
        val fileContent = file.inputStream.bufferedReader().use { it.readText() }
        val bronzeData = medallionService.processBronze(fileContent)
        val silverData = medallionService.processSilver(bronzeData)
        return medallionService.processGold(silverData)
    }
}
