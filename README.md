# Fatalities Data Warehouse Project

A containerised data warehouse solution for analyzing road safety data, featuring automated ETL, PostgreSQL storage, and BI tool integration.

## 🏁 Quick Start

### Prerequisites
- Docker & Docker Compose installed
- Web browser for pgAdmin (http://localhost:5051)
- Power BI/Tableau for visualizations

```bash
# Clone project and start services
git clone https://github.com/lainos123/CITS5504-Project-1.git
cd Project1-Fatalities
docker-compose up --build
```

## 🌐 Services Overview

| Service    | Port  | Credentials               | Purpose                  |
|------------|-------|---------------------------|--------------------------|
| PostgreSQL | 5433  | postgres/postgres         | Data warehouse storage   |
| pgAdmin    | 5051  | admin@admin.com/root      | Database management      |
| ETL        | -     | -                         | Automated data pipeline  |

## 📂 Directory Structure

```
.
├── Dockerfile
├── README.md
├── association_rules/
├── data
│   ├── geojson/
│   ├── processed/
│   └── raw
│       ├── LGA (count of dwellings).csv
│       ├── ardd_dictionary_sep2023.pdf
│       ├── bitre_fatal_crashes_dec2024.xlsx
│       └── bitre_fatalities_dec2024.xlsx
├── docker-compose.yml
├── images/
├── report/
├── requirements.txt
├── scripts
│   ├── etl_process.py
│   ├── queries.sql
│   └── setup.sh
├── visualisations/
└── working_notebooks/
```

## 🛠️ Full Setup Guide

### 1. Database Initialisation
```bash
`docker-compose up --build`
```

This command:
- 🐘 Creates PostgreSQL container with persistent storage
- ⚙️ Runs ETL pipeline to populate the warehouse
- 🌐 Launches pgAdmin at http://localhost:5051

### 2. pgAdmin Configuration
1. Login with `admin@admin.com`/`root`
2. Register new server:
   - **Name**: `DW_Project1`
   - **Host**: `pgdb`
   - **Port**: `5432`
   - **DB**: `datawarehouse`
   - **Credentials**: postgres/postgres

### 3. BI Tool Connection
**Power BI/Tableau Settings:**
- Server: `localhost`
- Port: `5433` 
- Database: `datawarehouse`
- Authentication: PostgreSQL (postgres/postgres)

## 🔍 Sample Analysis Workflow

1. Access pre-built queries in `scripts/queries.sql`
2. Connect BI tool to PostgreSQL instance
3. Create visualisations using the snowflake schema
4. Explore association rules in `outputs/association_rules.pdf`

## 🚨 Troubleshooting

### Database Connections
```bash
# Check container status
docker ps

# Restart services
docker-compose down && docker-compose up -d
```

### Python Environment
```bash
# Create local venv for development
bash scripts/setup.sh
```

### Common Issues
- Port conflicts: Ensure no local services use 5433/5051
- Data persistence: Docker volumes maintain data between restarts
- ETL errors: Check logs with `docker-compose logs etl`

## 📄 Documentation
- Full project report: `report/Project1_Report.pdf`
- ER Diagrams: `outputs/erd-pgadmin.png`
- Tableau workbooks: `visualizations/query_workbook.twb`

---

*Project by Laine Mulvay (22708032) & Mohammed Ismail Khan (24894389)*