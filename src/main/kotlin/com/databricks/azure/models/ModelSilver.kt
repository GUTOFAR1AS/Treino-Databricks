package com.databricks.azure.models

data class ModelSilver(
    val id: String?,
    val cleanedData: String?,
    val processDate: String?,
    val validationErrors: MutableList<String>?
)