import org.springframework.boot.gradle.tasks.run.BootRun

plugins {
	kotlin("jvm") version "2.2.21"
	kotlin("plugin.spring") version "2.2.21"
	id("org.springframework.boot") version "4.0.2"
	id("io.spring.dependency-management") version "1.1.7"
	kotlin("plugin.jpa") version "2.2.21"
}

group = "com.databricks"
version = "0.0.1-SNAPSHOT"
description = "Exemplo integração databricks"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(21)
	}
}

repositories {
	mavenCentral()
	maven { url = uri("https://repository.cloudera.com/artifactory/cloudera-repos/") }
}
//========================================================================================//
dependencies {
	implementation("org.springframework.boot:spring-boot-starter-jdbc")
	implementation("org.springframework.boot:spring-boot-starter-webmvc")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("tools.jackson.module:jackson-module-kotlin")
	implementation("com.databricks:databricks-jdbc:3.1.1")
	implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:2.3.0")
//========================================================================================//
	testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
//========================================================================================//
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
	compilerOptions {
		freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
	}
}

allOpen {
	annotation("jakarta.persistence.Entity")
	annotation("jakarta.persistence.MappedSuperclass")
	annotation("jakarta.persistence.Embeddable")
}

tasks.withType<Test> {
	useJUnitPlatform()
}
tasks.named<BootRun>("bootRun") {
	jvmArgs = listOf(
		"--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
	)
}
