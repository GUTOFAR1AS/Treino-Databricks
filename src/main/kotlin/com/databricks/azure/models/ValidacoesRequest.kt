package com.databricks.azure.models

data class ValidacoesRequest(
    val descricao: String,
    val cliente: String,
    val competencia: String,
)