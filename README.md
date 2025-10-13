# Wortmann2Shopify - Product Synchronization API

A comprehensive FastAPI application that synchronizes product data between Microsoft SQL Server databases and Shopify stores. This system supports bulk product imports, individual product synchronization, warranty management, and automated data processing from Wortmann FTP servers.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation & Setup](#installation--setup)
- [Configuration](#configuration)
- [API Documentation](#api-documentation)
- [Data Models](#data-models)
- [Usage Examples](#usage-examples)
- [Docker Deployment](#docker-deployment)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [Performance Optimization](#performance-optimization)
- [Security Considerations](#security-considerations)
- [Support](#support)
- [License](#license)

---

## Overview

This application serves as a bridge between your Microsoft SQL Server product database and Shopify e-commerce platform. It handles:

- **Product Synchronization**: Bidirectional sync between database and Shopify
- **Image Management**: Automatic image processing and upload to Shopify
- **Warranty System**: Complex warranty group management with price calculations
- **Batch Processing**: Efficient handling of large product catalogs
- **Data Import**: Automated import from Wortmann FTP servers
- **Real-time Updates**: Individual product updates and synchronization

### Key Benefits

- **Automated Workflows**: Reduces manual data entry and errors
- **Scalable Architecture**: Handles thousands of products efficiently
- **Flexible Configuration**: Supports various product types and warranty systems
- **Error Handling**: Comprehensive error handling and retry mechanisms
- **Monitoring**: Built-in health checks and connection testing
- **Docker Ready**: Easy deployment with Docker Compose

---

## Features

### Core Functionality
- **Product CRUD Operations**: Create, read, update, delete products
- **Bulk Operations**: Process multiple products in batches
- **Image Processing**: Convert hex/raw image data to base64 for Shopify
- **Warranty Management**: Complex warranty group calculations with price add-ons
- **Data Validation**: Comprehensive input validation using Pydantic models

### Integration Features
- **Shopify REST API**: Full integration with [Shopify's REST API](https://shopify.dev/docs/api/admin-rest)
- **Shopify GraphQL**: Advanced queries for bulk operations using [Shopify GraphQL Admin API](https://shopify.dev/docs/api/admin-graphql)
- **Microsoft SQL Server**: Direct database connectivity with ODBC
- **FTP Import**: Automated data import from Wortmann FTP servers

### Operational Features
- **Rate Limiting**: Built-in Shopify API rate limiting compliance
- **Error Recovery**: Automatic retry mechanisms with exponential backoff
- **Health Monitoring**: API and database connection health checks
- **Logging**: Comprehensive logging for debugging and monitoring

---

## Architecture

### Project Structure

```
app/
├── api/
│   ├── deps.py              # Dependency injection
│   └── endpoints/
│       ├── health.py        # Health check endpoints
│       ├── products.py      # Product management endpoints
│       └── wortmann.py      # Wortmann import endpoints
├── core/
│   ├── config.py           # Application configuration
│   └── database.py         # Database connection management
├── models/
│   ├── product.py          # Product data models
│   └── shopify.py          # Shopify-specific models
├── services/
│   ├── database_service.py # Database operations
│   ├── product_service.py  # Product processing logic
│   ├── shopify_service.py  # Shopify API integration
│   └── wortmann_service.py # Wortmann FTP import
├── utils/
│   └── helpers.py          # Utility functions
└── main.py                 # FastAPI application entry point
```

### Technology Stack

- **[FastAPI](https://fastapi.tiangolo.com/)**: Modern, fast web framework for building APIs
- **[Pydantic](https://docs.pydantic.dev/)**: Data validation and settings management
- **[PyODBC](https://github.com/mkleehammer/pyodbc)**: Microsoft SQL Server connectivity
- **[HTTPX](https://www.python-httpx.org/)**: Async HTTP client for API calls
- **[Docker](https://www.docker.com/)**: Containerization for easy deployment

### Data Flow

1. **Data Sources**:
   - Microsoft SQL Server (`WortmannProdukte`, `BilderShopify`, `Garantien` tables)
   - Wortmann FTP Server (CSV files and image archives)

2. **Processing Pipeline**:
   - Data extraction from multiple sources
   - Data merging and transformation
   - Shopify format conversion
   - Batch processing with rate limiting

3. **Output**:
   - Shopify store product catalog
   - Comprehensive logging and error reporting

---

## Installation & Setup

### Prerequisites

- [Python 3.11](https://www.python.org/downloads/) or higher
- Microsoft SQL Server (local or remote)
- [Shopify Partner Account](https://www.shopify.com/partners) or Custom App
- [Git](https://git-scm.com/)

### 1. Clone Repository

```bash
git clone <repository-url>
cd Wortmann2Shopify
```

### 2. Create Virtual Environment

```bash
python -m venv venv

# Windows
.\venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Install Microsoft ODBC Driver

#### Windows
1. Download **[Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)** from Microsoft
2. Run the installation
3. Verify: `odbcad32.exe`

#### Linux (Ubuntu/Debian)
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

#### macOS
```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
ACCEPT_EULA=Y brew install msodbcsql18
```

---

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# Database Configuration
DB_SERVER=your_sql_server_host
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_DRIVER=ODBC Driver 18 for SQL Server

# Shopify Configuration
SHOPIFY_SHOP_URL=https://your-shop.myshopify.com
SHOPIFY_ACCESS_TOKEN=your_access_token
SHOPIFY_API_VERSION=2024-10

# Wortmann FTP Configuration (Optional)
WORTMANN_FTP_HOST=ftp.wortmann.de
WORTMANN_FTP_PORT=21
WORTMANN_FTP_USER=your_ftp_username
WORTMANN_FTP_PASSWORD=your_ftp_password
WORTMANN_PATH_PRODUCTCATALOG=/Preisliste/productcatalog.csv
WORTMANN_PATH_CONTENT=/Preisliste/content.csv
WORTMANN_PATH_IMAGES_ZIP=/Produktbilder/productimages.zip

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=true

# Application Configuration
APP_NAME=Wortmann2Shopify API
VERSION=1.0.0
```

### Shopify Access Token Setup

1. Go to your [Shopify Admin Panel](https://admin.shopify.com/)
2. Navigate to Apps → Manage apps and sales channels
3. Create a new private app or [custom app](https://help.shopify.com/en/manual/apps/app-types/custom-apps)
4. Configure required permissions:
   - Read/Write Products
   - Read/Write Product Images
   - Read/Write Inventory
5. Copy the Access Token to your `.env` file

### Database Schema

The application expects these tables in your SQL Server database:

#### WortmannProdukte
```sql
CREATE TABLE WortmannProdukte (
    ProductId NVARCHAR(50) PRIMARY KEY,
    Title NVARCHAR(255),
    LongDescription NTEXT,
    DescriptionShort NVARCHAR(500),
    Manufacturer NVARCHAR(100),
    Category NVARCHAR(100),
    CategoryPath NVARCHAR(255),
    Price_B2C_inclVAT DECIMAL(10,2),
    Price_B2B_Regular DECIMAL(10,2),
    Price_B2B_Discounted DECIMAL(10,2),
    Stock INT,
    GrossWeight DECIMAL(8,3),
    NetWeight DECIMAL(8,3),
    Warranty NVARCHAR(100),
    Garantiegruppe INT,
    StockNextDelivery NVARCHAR(100),
    AccessoryProducts NVARCHAR(500),
    -- Additional fields as needed
);
```

#### BilderShopify
```sql
CREATE TABLE BilderShopify (
    supplier_aid NVARCHAR(50),
    base64 NTEXT,
    hex NTEXT,
    IsPrimary INT DEFAULT 0
);
```

#### Garantien
```sql
CREATE TABLE Garantien (
    id INT PRIMARY KEY,
    name NVARCHAR(100),
    monate INT,
    prozentsatz DECIMAL(5,2),
    minimum DECIMAL(10,2),
    garantiegruppe INT
);
```

---

## API Documentation

### Base URL
```
http://localhost:8000
```

### Interactive Documentation
- **Swagger UI**: `http://localhost:8000/api/v1/docs`
- **ReDoc**: `http://localhost:8000/api/v1/redoc`

### Authentication
All endpoints require proper Shopify access token configuration. The API uses the token from environment variables.

---

## Data Models

### Product Models

#### ProductBase
```python
class ProductBase(BaseModel):
    ProductId: str
    Title: Optional[str] = None
    LongDescription: Optional[str] = None
    DescriptionShort: Optional[str] = None
    Manufacturer: Optional[str] = None
    Category: Optional[str] = None
    CategoryPath: Optional[str] = None
    Price_B2C_inclVAT: Optional[float] = None
    Price_B2B_Regular: Optional[float] = None
    Stock: Optional[int] = None
    GrossWeight: Optional[float] = None
    NetWeight: Optional[float] = None
    Warranty: Optional[str] = None
    Garantiegruppe: Optional[int] = None
```

#### ShopifyProduct
```python
class ShopifyProduct(BaseModel):
    title: str
    handle: str
    body_html: str
    vendor: Optional[str] = None
    product_type: Optional[str] = None
    tags: Optional[List[str]] = None
    variants: List[ShopifyVariant]
    options: List[ShopifyOption]
    metafields: List[ShopifyMetafield]
    images: Optional[List[ShopifyImage]] = None
```

### Request Models

#### WorkflowRequest
```python
class WorkflowRequest(BaseModel):
    product_limit: int = Field(default=None)
    image_limit: int = Field(default=None)
    batch_size: int = Field(default=None)
    dry_run: bool = Field(default=False)
```

#### SyncProductsRequest
```python
class SyncProductsRequest(BaseModel):
    product_ids: List[str] = Field(..., description="List of product IDs to sync")
    dry_run: bool = Field(default=False, description="Dry run mode")
    batch_size: int = Field(default=5, description="Batch processing size")
```

---

## Usage Examples

### 1. Health Check

```bash
curl -X GET http://localhost:8000/api/v1/health
```

**Response:**
```json
{
  "status": "healthy",
  "service": "fastapi-n8n-workflow",
  "version": "1.0.0"
}
```

### 2. Test Connections

```bash
curl -X GET http://localhost:8000/api/v1/test-connections
```

**Response:**
```json
{
  "database": "connected",
  "shopify": "connected"
}
```

### 3. Upload All Products (Dry Run)

```bash
curl -X POST http://localhost:8000/api/v1/products/upload-all-products \
  -H "Content-Type: application/json" \
  -d '{
    "dry_run": true,
    "batch_size": 10,
    "product_limit": 100
  }'
```

**Response:**
```json
{
  "status": "success",
  "message": "Dry run completed successfully",
  "total_products": 100,
  "execution_time": 2.34,
  "results": [{
    "sample_product_count": 100,
    "sample_product_handle": "prod-eu1009805"
  }]
}
```

### 4. Upload All Products (Live)

```bash
curl -X POST http://localhost:8000/api/v1/products/upload-all-products \
  -H "Content-Type: application/json" \
  -d '{
    "dry_run": false,
    "batch_size": 20
  }'
```

**Response:**
```json
{
  "status": "completed",
  "message": "Upload completed. 150 successful, 5 failed",
  "total_products": 155,
  "successful_uploads": 150,
  "failed_uploads": 5,
  "execution_time": 45.67,
  "results": [...]
}
```

### 5. Sync Specific Products

```bash
curl -X POST http://localhost:8000/api/v1/products/update-products-by-ids \
  -H "Content-Type: application/json" \
  -d '{
    "product_ids": ["eu1009805", "eu1009806", "eu1009807"],
    "dry_run": false,
    "batch_size": 3
  }'
```

### 6. Create New Products

```bash
curl -X POST http://localhost:8000/api/v1/products/create-products-by-ids \
  -H "Content-Type: application/json" \
  -d '{
    "product_ids": ["eu1009808", "eu1009809"],
    "batch_size": 2
  }'
```

### 7. Delete Products

```bash
curl -X POST http://localhost:8000/api/v1/products/delete-products-by-ids \
  -H "Content-Type: application/json" \
  -d '{
    "product_ids": ["eu1009810", "eu1009811"],
    "batch_size": 2
  }'
```

### 8. Export Shopify Products

```bash
curl -X GET http://localhost:8000/api/v1/products/export-shopify-products
```

**Response:**
```json
[
  {
    "ProductId": "eu1009805",
    "Title": "Example Product",
    "DescriptionShort": "Short description",
    "LongDescription": "<p>Detailed HTML description</p>",
    "Manufacturer": "Wortmann",
    "Category": "Notebooks",
    "CategoryPath": "Notebooks|Business",
    "Warranty": "3 Jahre Garantie",
    "Price_B2B_Regular": 899.99,
    "Price_B2B_Discounted": 849.99,
    "Price_B2C_inclVAT": 1071.49,
    "Currency": "EUR",
    "VATRate": 19,
    "Stock": 15,
    "StockNextDelivery": "2024-02-15",
    "ImagePrimary": "https://cdn.shopify.com/...",
    "ImageAdditional": "https://cdn.shopify.com/...",
    "GrossWeight": 2.5,
    "NetWeight": 2.2,
    "NonReturnable": false,
    "EOL": false,
    "Promotion": false,
    "AccessoryProducts": "eu1009806|eu1009807",
    "Garantiegruppe": 1
  }
]
```

### 9. Wortmann Import

```bash
curl -X POST http://localhost:8000/api/v1/wortmann/wortmann-import
```

**Response:**
```json
{
  "status": "completed",
  "message": "Wortmann import finished",
  "total_products": 1250,
  "successful_uploads": 1248,
  "failed_uploads": 0,
  "execution_time": 120.45,
  "results": [{
    "products_processed": 1250,
    "products_upserted": 1248,
    "images_processed": 3420,
    "warranties_processed": 156
  }]
}
```

---

## Docker Deployment

### Docker Compose Setup

The application includes a complete Docker setup with health checks and networking.

```bash
# Build and start all services
docker-compose up --build

# Start in background
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

### Docker Configuration

The `docker-compose.yml` includes:
- **Health Checks**: Automatic container health monitoring
- **Networking**: Isolated network for service communication
- **Environment**: Secure environment variable management
- **Restart Policy**: Automatic restart on failure

### Production Considerations

For production deployment:

1. **Security**:
   - Use [Docker secrets](https://docs.docker.com/engine/swarm/secrets/) for sensitive data
   - Configure proper CORS origins
   - Use HTTPS with SSL certificates

2. **Monitoring**:
   - Set up log aggregation ([ELK stack](https://www.elastic.co/elastic-stack))
   - Configure health check monitoring
   - Set up alerting for failures

3. **Scaling**:
   - Use [Docker Swarm](https://docs.docker.com/engine/swarm/) or [Kubernetes](https://kubernetes.io/)
   - Configure load balancing
   - Set up database connection pooling

---

## Troubleshooting

### Common Issues

#### Database Connection Errors

**Error**: `[Microsoft][ODBC Driver Manager] The data source name was not found`

**Solutions**:
1. Verify ODBC Driver installation
2. Check driver name in `.env` file
3. Test connection with ODBC test tool
4. Ensure SQL Server is accessible

#### Shopify API Errors

**Error**: `401 Unauthorized`

**Solutions**:
1. Verify access token in `.env` file
2. Check app permissions in Shopify admin
3. Ensure token hasn't expired
4. Test with [Shopify API](https://shopify.dev/docs/api) directly

**Error**: `429 Too Many Requests`

**Solutions**:
1. The application has built-in rate limiting
2. Increase delays between requests if needed
3. Use smaller batch sizes
4. Implement request queuing

#### Memory Issues

**Error**: `Out of memory` during bulk operations

**Solutions**:
1. Reduce `batch_size` parameter
2. Process products in smaller chunks
3. Increase Docker memory limits
4. Use streaming for large datasets

### Debugging Tips

1. **Enable Debug Mode**:
   ```env
   DEBUG=true
   ```

2. **Check Logs**:
   ```bash
   # Docker
   docker-compose logs -f app
   
   # Local
   tail -f logs/app.log
   ```

3. **Test Individual Components**:
   ```bash
   # Test database connection
   curl -X GET http://localhost:8000/api/v1/test-connections
   
   # Test with dry run first
   curl -X POST http://localhost:8000/api/v1/products/upload-all-products \
     -H "Content-Type: application/json" \
     -d '{"dry_run": true, "batch_size": 1}'
   ```

4. **Monitor Performance**:
   - Use application metrics
   - Monitor database query performance
   - Track Shopify API response times

---

## Development

### Local Development Setup

1. **Install Development Dependencies**:
   ```bash
   pip install -r requirements.txt
   pip install pytest pytest-asyncio httpx
   ```

2. **Run Development Server**:
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
   ```

3. **Run Tests**:
   ```bash
   pytest tests/
   ```

### Code Structure Guidelines

- **Services**: Business logic and external API interactions
- **Models**: Data validation and serialization
- **Endpoints**: HTTP request/response handling
- **Utils**: Reusable utility functions
- **Core**: Application configuration and database management

### Adding New Features

1. **Create Models**: Define Pydantic models in `app/models/`
2. **Implement Services**: Add business logic in `app/services/`
3. **Create Endpoints**: Add API routes in `app/api/endpoints/`
4. **Add Tests**: Create test files in `tests/`
5. **Update Documentation**: Update README and API docs

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

---

## Performance Optimization

### Batch Processing
- Use appropriate batch sizes (10-50 products per batch)
- Implement parallel processing for independent operations
- Use async/await for I/O operations

### Database Optimization
- Use connection pooling
- Optimize SQL queries with proper indexing
- Implement query result caching where appropriate

### API Rate Limiting
- Respect [Shopify's rate limits](https://shopify.dev/docs/api/usage/rate-limits) (2 requests per second)
- Implement exponential backoff for retries
- Use GraphQL for bulk operations when possible

### Memory Management
- Process large datasets in chunks
- Use generators for large result sets
- Implement proper cleanup of resources

---

## Security Considerations

### API Security
- Use HTTPS in production
- Implement proper authentication
- Validate all input data
- Sanitize database queries

### Data Protection
- Encrypt sensitive configuration data
- Use secure connection strings
- Implement audit logging
- Regular security updates

### Shopify Integration
- Use minimal required permissions
- Rotate access tokens regularly
- Monitor API usage
- Implement [webhook validation](https://shopify.dev/docs/apps/build/webhooks/subscribe/https#step-5-verify-the-webhook)

---

## Support

### Getting Help

1. **Check Documentation**: Review this README and API docs
2. **Check Logs**: Examine application and error logs
3. **Test Connections**: Use health check endpoints
4. **Create Issue**: Submit detailed issue reports

### Issue Reporting

When reporting issues, include:
- Application version
- Configuration (without sensitive data)
- Error messages and logs
- Steps to reproduce
- Expected vs actual behavior

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.