# worker

Componente responsable de la implementación del patrón **Outbox** para garantizar la entrega confiable de eventos a Kafka. Consulta periódicamente la tabla `outbox_event` en PostgreSQL y publica los eventos pendientes al topic correspondiente.

## Responsabilidades

- Polling periódico de eventos no procesados en la tabla `outbox_event`
- Publicación de eventos al topic de Kafka configurado
- Marcado de eventos como procesados tras publicación exitosa
- Manejo resiliente de errores con reintento automático en el siguiente ciclo

## Diagrama de Arquitectura

```mermaid
flowchart TB
    subgraph Docker Compose
        Client([Cliente HTTP]) -->|REST 8080| CS["ms-customer-service\n/api/ms-customer-service/clientes"]
        Client -->|REST 8081| AS["ms-account-service\n/api/ms-account-service/cuentas\n/movimientos · /reportes"]

        CS -->|JPA| DB_C[(PostgreSQL\nms_customer_db)]
        CS -->|Inserta evento| OT[(outbox_eventos)]

        W["worker\n(Outbox Poller)"] -->|Lee outbox| OT
        W -->|Publica| K{{Kafka\ncustomer-events}}

        AS -->|Consume| K
        AS -->|JPA| DB_A[(PostgreSQL\nms_account_db)]
        AS -->|Snapshot local| SN[(clientes_snapshot)]
    end

    style CS fill:#4CAF50,color:#fff
    style AS fill:#2196F3,color:#fff
    style W fill:#FF9800,color:#fff
    style K fill:#333,color:#fff
```

## Diagrama de flujo del Worker

```mermaid
flowchart LR
    DB[(PostgreSQL\noutbox_event)] -->|Poll cada N ms| Scheduler[OutboxPollerScheduler]
    Scheduler -->|Publica| Kafka[(Kafka\ncustomer-events)]
    Scheduler -->|Marca procesado| DB
```

## Despliegue con Docker Compose

Este componente se despliega junto con toda la infraestructura (PostgreSQL, Kafka, ms-customer-service y ms-account-service) mediante Docker Compose.
El archivo `docker-compose.yml` se encuentra en el repositorio **ms-customer-service**, dentro de la carpeta `docker/`.

```bash
# Desde el directorio ms-customer-service/docker
cd ../ms-customer-service/docker
docker compose up --build
```

Consulta la guía completa de despliegue en: **ms-customer-service/docker/README.md**

## Cómo ejecutar localmente (sin Docker)

### Prerrequisitos

| Herramienta | Versión mínima   |
|-------------|------------------|
| Java        | 17               |
| Gradle      | Wrapper incluido |
| PostgreSQL  | 13+              |
| Kafka       | 3.x+             |

> **Nota:** El worker requiere que la base de datos y Kafka estén disponibles. Se recomienda levantar la infraestructura con Docker Compose.

### 1. Configurar `application.properties`

Edita `src/main/resources/application.properties` con tus credenciales:

```properties
spring.application.name=worker

spring.datasource.url=jdbc:postgresql://localhost:5432/ms_customer_db
spring.datasource.username=tu_usuario
spring.datasource.password=tu_contrasena
spring.datasource.driver-class-name=org.postgresql.Driver

spring.jpa.hibernate.ddl-auto=none
spring.jpa.show-sql=false

spring.kafka.bootstrap-servers=localhost:9092

kafka.topics.customer-events=customer-events

outbox.poll.interval-ms=5000
```

### 2. Compilar el proyecto

> **Nota:** Este proyecto usa **Gradle Groovy DSL** (`build.gradle`). Requiere Gradle Wrapper incluido; no es necesario tener Gradle instalado globalmente.

```bash
./gradlew clean build
```

### 3. Ejecutar el proyecto

```bash
./gradlew bootRun
```

### 4. Ejecutar pruebas

```bash
./gradlew test
```

### 5. Calidad de código

```bash
# Análisis completo (tests + checkstyle + PMD + JaCoCo)
./gradlew clean test checkstyleMain pmdMain jacocoTestReport

# Ver reporte de cobertura
open build/reports/jacoco/html/index.html

# Ver reporte Checkstyle
open build/reports/checkstyle/main.html

# Ver reporte PMD
open build/reports/pmd/main.html
```

> Umbral mínimo de cobertura: **90%** (verificado con JaCoCo 0.8.12). Checkstyle 10.21.4 y PMD 7.10.0 se ejecutan sobre el código fuente principal.
