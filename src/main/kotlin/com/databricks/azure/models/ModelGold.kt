package com.databricks.azure.models

import java.io.Serializable

data class ModelGold(
    val idArquivo: String?,
    val nomeArquivo: String?,
    val finalData: String?,
    val aggregatedDate: String?,
    val businessRules: Map<String, Serializable>?,
)