package com.databricks.azure.models

import java.io.Serializable

data class ModelGold(
    val id: String?,
    val finalData: String?,
    val aggregatedDate: String?,
    val businessRules: Map<String, Serializable>?,
)