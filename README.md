# vsysWS19-20

- Spark
- Kafka
- Cassandra

## Cassandra (Persistence)
Wird verwendet zur Speicherung der Ergebnisse; Zuordnung von Experiment zu File

## Spark (Worker)
Zur Aufbereitung und Analyse der Daten jeweils ein Spark Job

## Kafka (Dispatcher)
Zur Kommunikation zwischen den verschiedenen Spark Jobs

## Zookeeper
Zwingendermaßen für Kafka

## Docker
Docker Images für:
- Spark Master
- Spark Worker
- Zookeeper
- Kafka
- Cassandra

Orchestrierung mithilfe von Kubernetes (?)

## Spring
Als Schnittstelle zum Nutzer

## Reporting Node
Was soll die Reporting Node machen? Health? Fortschritt? -> Marx fragen
