package com.databricks.azure.service.medallion

import com.databricks.azure.models.ModelBronze
import com.databricks.azure.models.ModelGold
import com.databricks.azure.models.ModelSilver

interface IMedallion {
    fun processBronze(fileContent: String, fileName: String): List<ModelBronze>
    fun processSilver(bronzeData: List<ModelBronze>): List<ModelSilver>
    fun processGold(silverData: List<ModelSilver>, fileName: String): List<ModelGold>
    fun saveGold(idArquivo: String, nomeArquivo: String): List<ModelGold>
    fun saveGoldData(goldDataList: List<ModelGold>): List<ModelGold>
    fun createTableGold(): ModelGold
}