All rights reserved. This code is not licensed for reuse, distribution, or commercial use.

# Retail Supply Chain Analytics & Dashboard

Flask web app for retail analytics and demand forecasting. 
End-to-end data platform for demand forecasting, inventory optimization, and retail KPI analysis.


## Business Problem
Retail companies struggle with:
    - Poor demand forecasting
    - Overstock and stockouts
    - Lack of real-time KPI visibility

    This leads to lost sales and inefficient inventory allocation.\


## Solution
This application provides:
    - 📊 Automated KPI dashboard (sales, margins, inventory rotation)
    - 📦 Inventory tracking and stock health metrics
    - 📈 Demand forecasting using:
        -> Prophet (trend & seasonality)
        -> Random Forest (feature-driven corrections)
    - 🚛 Replenishment suggestions
    - ⚙️ Scalable data pipeline for ingesting retail datasets


## Architecture
- Backend: Flask
- Database: PostgreSQL
- Data Processing: Pandas
- ML Models:
  - Prophet (baseline forecasting)
  - Random Forest (residual modeling)
- Task Queue: Redis + RQ
- Deployment-ready with Docker


## Diagrama
    PENDIENTE


## Key Features

- Multi-tenant data model (user-based isolation)
- Bulk data ingestion (CSV upload with validation)
- Automatic SKU catalog creation (data integrity)
- Advanced feature engineering:
  - Lag features
  - Rolling statistics
  - Calendar effects
  - Inventory-based features
- Retail KPIs:
  - Sell-through
  - Inventory rotation
  - Gross margin


## Machine Learning Approach

A hybrid ensemble approach is used:

1. Prophet models trend and seasonality
2. Residuals are computed
3. Random Forest learns corrections using:
   - Price changes
   - Promotions (inferred)
   - Inventory levels

Final prediction:
Forecast = Prophet + ML residual correction


## Data Pipeline

1. Upload CSV files (sales, inventory, catalog)
2. Column normalization with synonym mapping
3. Data validation and cleaning
4. SKU upsert into catalog
5. Bulk insert into database


# 📈 Retail Forecast Dashboard & AI Engine

Una aplicación web asíncrona diseñada para automatizar el pronóstico de demanda y la asignación de inventario en el sector Retail, utilizando modelos de Machine Learning (Prophet, Random Forest).

## 🏗️ Arquitectura del Sistema

Este proyecto utiliza una arquitectura de procesamiento en segundo plano (Background Tasks) para manejar el entrenamiento pesado de los modelos sin bloquear la interfaz del usuario.

* **Frontend/Backend API:** Flask (Python)
* **Message Broker:** Redis (Dockerizado)
* **Task Queue / Worker:** RQ (Redis Queue)
* **Machine Learning:** Pandas, Prophet/Random Forest, Scikit-Learn
* **Entorno Recomendado:** WSL (Ubuntu 22.04) en Windows

## ⚙️ Requisitos Previos

* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Para ejecutar Redis localmente)
* WSL 2 con Ubuntu 22.04 (Si desarrollas en Windows)
* Python 3.10+

## 🚀 Instalación y Ejecución Local

### 1. Preparar la Base de Datos (Redis)
Inicia tu Docker Desktop y ejecuta el servidor de Redis en el puerto 6379:
```bash
docker run -d -p 6379:6379 redis