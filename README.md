# Shopify API - Produkt-Upload System

Dieses Projekt ermöglicht es, Produkte aus einer Microsoft SQL Server Datenbank in Shopify zu importieren. Es verwendet FastAPI für die API und unterstützt Batch-Uploads von Produkten mit Bildern und Garantien.

## 📋 Voraussetzungen

- Python 3.11 oder höher
- Microsoft SQL Server (lokal oder remote)
- Shopify Partner Account oder Custom App
- Git

## 🚀 Installation

### 1. Repository klonen

```bash
git clone <repository-url>
cd shopify_api
```

### 2. Virtuelle Umgebung erstellen

```bash
python -m pip install virtualenv
python -m venv venv
```

### 3. Virtuelle Umgebung aktivieren

**Windows:**
```bash
.\venv\Scripts\activate
```

**Linux/Mac:**
```bash
source venv/bin/activate
```

### 4. Abhängigkeiten installieren

```bash
pip install -r requirements.txt
```

## 🔧 MSSQL Driver Installation

### Windows
1. Laden Sie den **Microsoft ODBC Driver 18 for SQL Server** von der Microsoft Website herunter
2. Führen Sie die Installation aus
3. Überprüfen Sie die Installation:
   ```bash
   odbcad32.exe
   ```

### Linux (Ubuntu/Debian)
```bash
# Docker (empfohlen)
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=YourPassword123!" \
  -p 1433:1433 --name sql1 --hostname sql1 \
  -d mcr.microsoft.com/mssql/server:2022-latest

# Oder lokale Installation
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

### macOS
```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
ACCEPT_EULA=Y brew install msodbcsql18
```

## ⚙️ Konfiguration

### 1. .env Datei erstellen

Erstellen Sie eine `.env` Datei im Projektverzeichnis:

```env
# Database settings
DB_SERVER=localhost
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_DRIVER=ODBC Driver 18 for SQL Server

# Shopify settings
SHOPIFY_SHOP_URL=https://your-shop.myshopify.com
SHOPIFY_ACCESS_TOKEN=your_access_token

# API settings
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=true
```

### 2. Shopify Access Token erhalten

1. Gehen Sie zu Ihrem Shopify Admin Panel
2. Apps → App und Verkaufskanäle verwalten
3. Private Apps → Neue private App erstellen
4. Notieren Sie sich den Access Token

### 3. Datenbank-Verbindung testen

```bash
python -c "from app.core.database import db_manager; print('Connection successful' if db_manager.test_connection() else 'Connection failed')"
```

## 🚀 Verwendung

### Entwicklungsserver starten

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Mit Docker

```bash
# Build und starten
docker-compose up --build

# Nur starten
docker-compose up

# Im Hintergrund
docker-compose up -d
```

## 🧪 Test Data Setup

Before testing the API, you can populate your database with test data:

### 1. Insert Test Data
```bash
# Run the test data insertion script
python run_test_data.py
```

This will create 10 test products with the following structure:
- **Product IDs**: TEST001-TEST010
- **Categories**: Electronics, Gaming, Monitors, etc.
- **Prices**: €39.99 - €1,299.99
- **Warranties**: 12-60 months with different pricing
- **Images**: Simple 1x1 pixel PNG images (base64 encoded)

### 2. Verify Test Data
```bash
# Check if test data was inserted
curl -X GET http://localhost:8000/health
```

## 📡 API Endpoints

### 1. Workflow ausführen (All Products)
```bash
POST /execute-workflow
{
  "dry_run": false,
  "batch_size": 10
}
```

### 2. Test Workflow (Test Data Only)
```bash
POST /test-workflow
{
  "dry_run": true,
  "batch_size": 5
}
```

**Safe for testing**: This endpoint only processes products with `TEST` prefix.

### 3. Produkte synchronisieren (NEU!)
```bash
POST /sync-products
{
  "dry_run": true,
  "batch_size": 10
}
```

**Was macht die Sync-Funktion:**
- Vergleicht Produkte zwischen Datenbank und Shopify
- Erkennt geänderte Produkte (Preis, Lagerbestand, Beschreibung, etc.)
- Aktualisiert nur geänderte Produkte
- Erstellt neue Produkte, die in Shopify fehlen
- Zeigt eine detaillierte Analyse der Änderungen

**Sync-Analyse Beispiel:**
```json
{
  "status": "success",
  "message": "Sync analysis completed (dry run)",
  "results": [{
    "summary": {
      "total_db_products": 150,
      "total_shopify_products": 145,
      "products_to_update": 12,
      "products_to_create": 5,
      "unchanged_products": 133,
      "shopify_only_products": 0
    }
  }]
}
```

### 4. Health Check
```bash
GET /health
```

## 🔍 Fehlerbehebung

### Datenbank-Verbindungsfehler

**Fehler:** `[Microsoft][ODBC Driver Manager] The data source name was not found`

**Lösung:**
1. Stellen Sie sicher, dass der ODBC Driver installiert ist
2. Stellen Sie sicher, dass der Driver-Name in der .env Datei korrekt ist
3. Testen Sie die Verbindung mit einem ODBC-Testtool

### Shopify API Fehler

**Fehler:** `401 Unauthorized`

**Lösung:**
1. Überprüfen Sie den Access Token in der .env Datei
2. Stellen Sie sicher, dass die App die richtigen Berechtigungen hat
3. Testen Sie die Verbindung mit dem Shopify Admin API

### Batch Size Fehler

**Fehler:** `range() arg 3 must not be zero`

**Lösung:**
- Der Code wurde bereits aktualisiert, um diesen Fehler zu verhindern
- Stellen Sie sicher, dass Sie die neueste Version verwenden

## 🧪 Testing the Update Function

### 1. Dry Run Test (Recommended First Step)
```bash
# Test with dry run to see what would be processed
curl -X POST http://localhost:8000/test-workflow \
  -H "Content-Type: application/json" \
  -d '{
    "dry_run": true,
    "batch_size": 5,
    "product_limit": 10
  }'
```

**Expected Response:**
```json
{
  "status": "success",
  "message": "Test workflow dry run completed successfully",
  "total_products": 10,
  "execution_time": 1.23,
  "results": [{
    "test_products": 10,
    "sample_product_handle": "prod-TEST001",
    "product_ids": ["prod-TEST001", "prod-TEST002", "prod-TEST003", "prod-TEST004", "prod-TEST005"]
  }]
}
```

### 2. Live Test (Actual Shopify Upload)
```bash
# Test with actual upload to Shopify
curl -X POST http://localhost:8000/test-workflow \
  -H "Content-Type: application/json" \
  -d '{
    "dry_run": false,
    "batch_size": 2
  }'
```

**Expected Response:**
```json
{
  "status": "completed",
  "message": "Test workflow completed. 10 successful, 0 failed",
  "total_products": 10,
  "successful_uploads": 10,
  "failed_uploads": 0,
  "execution_time": 15.67,
  "results": [...]
}
```

### 3. Monitor Progress
```bash
# Watch the logs in real-time
docker-compose logs -f app

# Check health status
curl -X GET http://localhost:8000/health
```

## 📊 Monitoring

### Logs anzeigen
```bash
# Docker
docker-compose logs -f

# Lokal
tail -f logs/app.log
```

### API Dokumentation
- Swagger UI: http://localhost:8000/api/v1/docs
- ReDoc: http://localhost:8000/api/v1/redoc

## 🔄 Automatisierung

### Cron Job für automatische Synchronisation
```bash
# Täglich um 2:00 Uhr synchronisieren
0 2 * * * curl -X POST http://localhost:8000/sync-products \
  -H "Content-Type: application/json" \
  -d '{"dry_run": false, "batch_size": 20}'
```

### Docker Cron
```dockerfile
# In Dockerfile hinzufügen
RUN apt-get update && apt-get install -y cron
COPY sync-cron /etc/cron.d/sync-cron
RUN chmod 0644 /etc/cron.d/sync-cron
RUN crontab /etc/cron.d/sync-cron
CMD ["cron", "-f"]
```

## 🤝 Beitragen

1. Fork das Repository
2. Erstellen Sie einen Feature Branch
3. Committen Sie Ihre Änderungen
4. Pushen Sie zum Branch
5. Erstellen Sie einen Pull Request

## 📄 Lizenz

Dieses Projekt ist unter der MIT Lizenz lizenziert.

## 🆘 Support

Bei Fragen oder Problemen:
1. Überprüfen Sie die Logs
2. Testen Sie die Verbindungen
3. Erstellen Sie ein Issue im Repository
