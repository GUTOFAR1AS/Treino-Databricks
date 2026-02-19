package com.databricks.azure.service

import com.databricks.azure.models.JobInfo
import com.databricks.azure.models.ValidacoesRequest

interface DatabricksImpl {
    fun enviarDados(dados: List<ValidacoesRequest>)
    fun consultarJobs(): List<JobInfo>
    fun consultarDeltaTable(schema: String, tabela: String, limite: Int): List<Map<String, Any>>
}