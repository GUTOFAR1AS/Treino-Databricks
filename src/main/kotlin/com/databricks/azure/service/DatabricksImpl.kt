package com.databricks.azure.service

import com.databricks.azure.models.JobInfo
import com.databricks.azure.models.ValidacoesRequest

interface DatabricksImpl {
    fun sendDados(dados: List<ValidacoesRequest>)
    fun getJobs(): List<JobInfo>
    fun getDeltaTable(schema: String, tabela: String, limite: Int): List<Map<String, Any>>
    fun getAllDeltaTables(): List<String>
    fun startCluster(clusterId: String)
    fun stopCluster(clusterId: String)
}