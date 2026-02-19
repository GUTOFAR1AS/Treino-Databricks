package com.databricks.azure.models

data class JobInfo(
    val nome: String,
    val criador: String,
    val notebookPath: String?,
    val temConfiguracoes: Boolean,
    val configuracoes: Map<String, Any>?
)
