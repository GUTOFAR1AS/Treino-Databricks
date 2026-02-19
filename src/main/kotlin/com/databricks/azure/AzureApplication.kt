package com.databricks.azure

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class AzureApplication

fun main(args: Array<String>) {
	runApplication<AzureApplication>(*args)
}
