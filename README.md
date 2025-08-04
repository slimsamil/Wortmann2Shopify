# Shopify API - Product Upload System

This project enables importing products from a Microsoft SQL Server database into Shopify. It uses FastAPI for the API and supports batch uploads of products with images and warranties.

## 📋 Prerequisites

- Python 3.11 or higher
- Microsoft SQL Server (local or remote)
- Shopify Partner Account or Custom App
- Git

## 🚀 Installation

### 1. Clone repository

```bash
git clone <repository-url>
cd shopify_api
```

### 2. Create virtual environment

```bash
python -m pip install virtualenv
python -m venv venv
```

### 3. Activate virtual environment

**Windows:**
```bash
.\venv\Scripts\activate
```

**Linux/Mac:**
```bash
source venv/bin/activate
```

### 4. Install dependencies

```bash
pip install -r requirements.txt
```

## 🔧 MSSQL Driver Installation

### Windows
1. Download the **Microsoft ODBC Driver 18 for SQL Server** from the Microsoft website
2. Run the installation
3. Verify the installation:
   ```bash
   odbcad32.exe
   ```

### Linux (Ubuntu/Debian)
```bash
# Docker (recommended)
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=YourPassword123!" \
  -p 1433:1433 --name sql1 --hostname sql1 \
  -d mcr.microsoft.com/mssql/server:2022-latest

# Or local installation
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

## ⚙️ Configuration

### 1. Create .env file

Create a `.env` file in the project directory:

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

### 2. Get Shopify Access Token

1. Go to your Shopify Admin Panel
2. Apps → Manage apps and sales channels
3. Private Apps → Create new private app
4. Note down the Access Token

### 3. Test database connection

```bash
python -c "from app.core.database import db_manager; print('Connection successful' if db_manager.test_connection() else 'Connection failed')"
```

## 🚀 Usage

### Start development server

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### With Docker

```bash
# Build and start
docker-compose up --build

# Start only
docker-compose up

# In background
docker-compose up -d
```

## 📡 API Endpoints

### Workflow Endpoints

#### 1. Sync Products by IDs
**POST** `/api/v1/workflow/sync-products-by-ids`

Synchronizes multiple products by their IDs between database and Shopify.

**Parameters:**
```json
{
  "product_ids": ["eu1009805", "eu1009806"],
  "dry_run": false,
  "create_if_missing": true,
  "batch_size": 10
}
```

**Functions:**
- Loads specific products from database
- Transforms them to Shopify REST API format
- Updates them in Shopify via PUT requests
- Handles both existing (updates) and new products (creates)
- Batch processing with rate limiting

**Response:**
```json
{
  "status": "completed",
  "message": "Sync completed: 8 successful, 2 failed",
  "total_products": 10,
  "successful_uploads": 8,
  "failed_uploads": 2,
  "execution_time": 15.67,
  "results": [...]
}
```

#### 2. Sync Single Product
**POST** `/api/v1/workflow/sync-single-product`

Synchronizes a single product between database and Shopify.

**Parameters:**
```json
{
  "product_id": "eu1009805",
  "dry_run": false,
  "create_if_missing": true
}
```

**Functions:**
- Loads the specific product from database
- Checks if it exists in Shopify
- Compares data and detects changes
- Updates or creates the product in Shopify

**Response:**
```json
{
  "status": "completed",
  "message": "Product eu1009805 updated successfully",
  "total_products": 1,
  "successful_uploads": 1,
  "failed_uploads": 0,
  "execution_time": 2.34,
  "results": [...]
}
```

#### 3. Execute Workflow
**POST** `/api/v1/workflow/execute-workflow`

Executes the complete workflow for all products.

**Parameters:**
```json
{
  "dry_run": false,
  "batch_size": 20,
  "product_limit": null,
  "image_limit": null
}
```

**Functions:**
- Loads all products, images and warranties from database
- Performs data merging
- Processes products into Shopify format
- Sends products in batches to Shopify

#### 4. Sync Products
**POST** `/api/v1/workflow/sync-products`

Synchronizes products between database and Shopify with comparison.

**Parameters:**
```json
{
  "dry_run": false,
  "batch_size": 20
}
```

**Functions:**
- Loads products from both sources (database and Shopify)
- Compares and updates only changed products
- Creates new products that are missing in Shopify

### Health Check
**GET** `/health`

Checks the status of the API and database connection.

## 🔄 Sync Functionality

**What the sync function does:**
- Compares products between database and Shopify
- Detects changed products (price, stock, description, etc.)
- Updates only changed products
- Creates new products that are missing in Shopify
- Shows a detailed analysis of changes

**Sync Analysis Example:**
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

## 🔍 Troubleshooting

### Database Connection Errors

**Error:** `[Microsoft][ODBC Driver Manager] The data source name was not found`

**Solution:**
1. Make sure the ODBC Driver is installed
2. Make sure the driver name in the .env file is correct
3. Test the connection with an ODBC test tool

### Shopify API Errors

**Error:** `401 Unauthorized`

**Solution:**
1. Check the Access Token in the .env file
2. Make sure the app has the correct permissions
3. Test the connection with the Shopify Admin API

### Batch Size Errors

**Error:** `range() arg 3 must not be zero`

**Solution:**
- The code has already been updated to prevent this error
- Make sure you are using the latest version

## 🧪 Testing the Update Function

### 1. Dry Run Test (Recommended First Step)
```bash
# Test with dry run to see what would be processed
curl -X POST http://localhost:8000/api/v1/workflow/test-workflow \
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
curl -X POST http://localhost:8000/api/v1/workflow/test-workflow \
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

### View logs
```bash
# Docker
docker-compose logs -f

# Local
tail -f logs/app.log
```

### API Documentation
- Swagger UI: http://localhost:8000/api/v1/docs
- ReDoc: http://localhost:8000/api/v1/redoc

## 🔄 Automation

### Cron Job for automatic synchronization
```bash
# Synchronize daily at 2:00 AM
0 2 * * * curl -X POST http://localhost:8000/sync-products \
  -H "Content-Type: application/json" \
  -d '{"dry_run": false, "batch_size": 20}'
```

### Docker Cron
```dockerfile
# Add to Dockerfile
RUN apt-get update && apt-get install -y cron
COPY sync-cron /etc/cron.d/sync-cron
RUN chmod 0644 /etc/cron.d/sync-cron
RUN crontab /etc/cron.d/sync-cron
CMD ["cron", "-f"]
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a pull request

## 📄 License

This project is licensed under the MIT License.

## 🆘 Support

For questions or issues:
1. Check the logs
2. Test the connections
3. Create an issue in the repository
