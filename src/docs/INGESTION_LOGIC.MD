# Ingestion Logic

The pipeline ingests the `inventory` and `reservations` from the folders:

The pipeline consists of two ingestion logic:
- **Ingest inventory** : To keep the system up-to-date, the pipeline supports ingesting new inventory files and update 
inventory table. The pipeline can process **ONLY** one CSV file at a time to prevent inconsistent data.
- **Ingest reservations** : The pipeline can process multiple reservations JSON files during the execution. 

### Ingestion Logics

## Pipeline Logic
![RPG Pipeline Overall Ingestion Logic](img/RPG_Pipeline_Logic.png)

## Inventory Ingestion Logic
![Inventory Ingestion Logic](img/Inventory_Ingestion_Logic.png)

## Reservations Ingestion Logic
![Reservations Ingestion Logic](img/Reservation_Ingestion_Logic.png)