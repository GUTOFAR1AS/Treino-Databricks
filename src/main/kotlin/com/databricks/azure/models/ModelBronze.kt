package com.databricks.azure.models

data class ModelBronze(
    val id: String? = null,
    val rawData: String,
    val loadDate: String,
    val isValid: Boolean
)