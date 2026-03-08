# Skardi Server Demo: Product Search Pipeline

This demo showcases the Skardi Online Serving Pipeline's ability to serve SQL queries over a products dataset. The demo includes a pre-configured pipeline that allows filtering and searching through a comprehensive product catalog.

## Dataset Overview

The demo uses a `products.csv` dataset with 10,000+ product records containing:
- **Product Information**: ID, Name, Description, Brand, Category
- **Pricing**: Price (USD), Currency
- **Inventory**: Stock quantity, Availability status
- **Product Details**: Color, Size, EAN code
- **Internal Data**: Internal ID for tracking

## Demo Components

- **`pipeline.yaml`**: Defines the SQL query pipeline with filtering parameters
- **`ctx.yaml`**: Registers the products.csv dataset as a data source
- **This README**: Usage instructions and example queries


### Schema Structure
```yaml
# Simple, minimal schema - everything else is inferred automatically
metadata:
  name: "product-search-demo"
  version: "1.0.0"
  description: "Demonstrates SQL serving pipeline"

query: |
  SELECT columns FROM table
  WHERE condition = {parameter}
  LIMIT {limit}
```

### What's Automatically Inferred
- **Request parameters**: Extracted from `{parameter}` placeholders in SQL
- **Parameter types**: Inferred from SQL context and table schema
- **Response fields**: Extracted from SELECT clause with proper types
- **Nullability**: Determined from SQL structure and constraints

## Prerequisites

1. **Build the server**: Ensure the Skardi server is compiled
   ```bash
   cd skardi
   cargo build --bin skardi-server
   ```

2. **Dataset location**: Verify the products.csv file exists at `data/products.csv` relative to the project root

## Starting the Demo Server

Navigate to the project root and start the server with the demo configuration:

```bash
cargo run --bin skardi-server -- \
  --pipeline demo/pipeline.yaml \
  --ctx demo/ctx.yaml \
  --port 8080
```

You should see output similar to:
```
Starting Skardi Online Serving Pipeline Server
CLI Arguments parsed successfully
   Pipeline file: Some("demo/pipeline.yaml")
   Context file: Some("demo/ctx.yaml")
   Port: 8080
Loading server configuration...
Server configuration loaded successfully
   Pipeline: product-search-demo
   Data sources: 1
Starting HTTP server...
Server listening on 0.0.0.0:8080
```

## Available Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Service health check |
| `/health/:name` | GET | Per-pipeline health check |
| `/pipelines` | GET | List all registered pipelines |
| `/pipeline/:name` | GET | Get specific pipeline info |
| `/register_pipeline` | POST | Register a new pipeline |
| `/data_source` | GET | List all data sources |
| `/:name/execute` | POST | Execute a pipeline by name |

### 1. Service Health Check
```bash
curl http://localhost:8080/health
```

**Response:**
```json
{
  "status": "healthy",
  "service": "skardi-server",
  "timestamp": "2025-01-15T12:00:00.000Z"
}
```

### 2. Pipeline Health Check
```bash
curl http://localhost:8080/health/product-search-demo
```

**Response:**
```json
{
  "status": "healthy",
  "pipeline": {
    "name": "product-search-demo",
    "version": "1.0.0",
    "parameters": ["brand", "max_price", "min_price", "color", "category", "availability", "limit"]
  },
  "data_sources": {
    "total": 1,
    "healthy": 1,
    "checks": [
      {"name": "products", "status": "healthy", "accessible": true}
    ]
  },
  "health_check_time_ms": 2,
  "timestamp": "2025-01-15T12:00:00.000Z"
}
```

### 3. List Pipelines
```bash
curl http://localhost:8080/pipelines
```

**Response:**
```json
{
  "success": true,
  "pipelines": [
    {
      "name": "product-search-demo",
      "version": "1.0.0",
      "endpoint": "/product-search-demo/execute"
    }
  ],
  "count": 1,
  "data_sources": 1,
  "timestamp": "2025-01-15T12:00:00.000Z"
}
```

## Demo Query Examples

### Example 1: Get Top 5 Cheapest Products
```bash
curl -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{
    "brand": null,
    "min_price": null,
    "max_price": null,
    "color": null,
    "category": null,
    "availability": null,
    "limit": 5
  }' | jq .
```

**Expected Response:**
```json
{
  "success": true,
  "data": [
    {
      "product_id": 13,
      "product_name": "Scooter Bicycle Oven",
      "description": "Deal event item ever financial home.",
      "brand": "Mclean-Aguilar",
      "category": "Laptops & Computers",
      "price": 79,
      "currency": "USD",
      "stock_quantity": 203,
      "color": "LightSkyBlue",
      "size": "30x40 cm",
      "availability_status": "in_stock"
    }
  ],
  "row_count": 5,
  "execution_time_ms": 15
}
```

### Example 2: Filter by Brand (Exact Match)
```bash
curl -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{
    "brand": "Frye Group",
    "min_price": null,
    "max_price": null,
    "color": null,
    "category": null,
    "availability": null,
    "limit": 5
  }' | jq .
```

### Example 3: Filter by Price (Less Than)
```bash
curl -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{
    "brand": null,
    "max_price": 50.0,
    "color": null,
    "limit": 8
  }' | jq .
```

### Example 4: Filter by Color (Exact Match)
```bash
curl -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{
    "brand": null,
    "max_price": null,
    "color": "Black",
    "limit": 6
  }' | jq .
```

### Example 5: Combined Brand and Price Filter
```bash
curl -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{
    "brand": "Frye Group",
    "max_price": 300.0,
    "color": null,
    "limit": 5
  }' | jq .
```

### Example 6: Price Range Filter (Using min_price)
```bash
curl -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{
    "brand": null,
    "min_price": 100.0,
    "max_price": 300.0,
    "color": null,
    "category": null,
    "availability": null,
    "limit": 5
  }' | jq .
```

### Example 7: Filter by Category
```bash
curl -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{
    "brand": null,
    "min_price": null,
    "max_price": null,
    "color": null,
    "category": "Laptops & Computers",
    "availability": null,
    "limit": 8
  }' | jq .
```

### Example 8: Filter by Availability Status
```bash
curl -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{
    "brand": null,
    "min_price": null,
    "max_price": null,
    "color": null,
    "category": null,
    "availability": "in_stock",
    "limit": 6
  }' | jq .
```

### Example 9: All Filters Combined
```bash
curl -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{
    "brand": "Frye Group",
    "min_price": 50.0,
    "max_price": 300.0,
    "color": "Tan",
    "category": null,
    "availability": "in_stock",
    "limit": 5
  }' | jq .
```

## Query Parameters Reference

The pipeline accepts the following parameters (all automatically inferred from the SQL query):

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `brand` | string | No | Filter by exact brand name match | `"Apple"`, `"Samsung"` |
| `max_price` | float | No | Filter products with price less than this amount | `100.0`, `500.0` |
| `min_price` | float | No | Filter products with price greater than or equal to this amount | `50.0`, `25.0` |
| `color` | string | No | Filter by exact color match | `"Black"`, `"White"` |
| `category` | string | No | Filter by product category | `"Laptops & Computers"`, `"Kitchen Appliances"` |
| `availability` | string | No | Filter by availability status | `"in_stock"`, `"limited_stock"` |
| `limit` | integer | Yes | Maximum results to return | `5`, `10` |

## Filter Examples from Dataset

### Sample Brands (Exact Match Required)
- `"Frye Group"` - Multi-category products (tested and confirmed)
- `"Daniel and Sons"` - Home & electronics
- `"Schmitt-Foley"` - Technology products
- `"Baxter LLC"` - Electronics & accessories

### Sample Colors (Exact Match Required)
- `"Black"` - Classic color option (tested and confirmed)
- `"Tan"` - Neutral earth tone (tested and confirmed)
- `"Coral"` - Vibrant accent color
- `"RoyalBlue"` - Bold blue variant

### Price Range Information
- Prices in the dataset range from $1 to ~$999+
- Use `max_price` to find products under your budget (e.g., `50.0` for budget items)
- Use `min_price` to find products above a minimum price (e.g., `100.0` for premium items)
- Combine both for price range filtering (e.g., `min_price: 50.0, max_price: 300.0`)
- Tested price points: $50 (budget items), $300 (mid-range products)

### Sample Categories (Exact Match Required)
- `"Laptops & Computers"` - Technology and computing devices
- `"Kitchen Appliances"` - Home kitchen equipment
- `"Sports & Outdoors"` - Athletic and outdoor gear
- `"Home & Garden"` - Home improvement and gardening items
- `"Health & Beauty"` - Personal care and wellness products

### Sample Availability Statuses (Exact Match Required)
- `"in_stock"` - Items immediately available for purchase
- `"limited_stock"` - Items with low inventory levels
- `"pre_order"` - Items available for advance ordering
- `"out_of_stock"` - Currently unavailable items
- `"discontinued"` - Items no longer being produced


## Error Handling

The API returns detailed JSON error responses with appropriate HTTP status codes:

- **200 OK**: Successful query execution
- **400 Bad Request**: Invalid request parameters or malformed JSON
- **500 Internal Server Error**: Server-side execution errors

### Error Response Format

All error responses follow this structure:

> **Security Note**: Error responses are designed to provide helpful debugging information while avoiding exposure of sensitive details like SQL queries or internal system information that could be exploited.
```json
{
  "success": false,
  "error": "Human-readable error message",
  "error_type": "error_category",
  "details": {
    "additional_debug_information": "..."
  },
  "timestamp": "2025-01-15T12:00:00.000Z"
}
```

### Example Error Responses

**Missing Parameters (400 Bad Request):**
```json
{
  "success": false,
  "error": "Missing required parameters: limit",
  "error_type": "parameter_validation_error",
  "details": {
    "expected_parameters": ["limit"],
    "missing_parameters": ["limit"],
    "received_parameters": []
  },
  "timestamp": "2025-01-15T12:00:00.000Z"
}
```

**SQL Execution Error (500 Internal Server Error):**
```json
{
  "success": false,
  "error": "SQL query execution failed: table 'products' not found",
  "error_type": "query_execution_error",
  "details": {
    "engine_error": "table 'datafusion.public.products' not found",
    "registered_tables": "Check server logs for data source registration status",
    "suggestion": "Verify that data sources are properly registered and accessible"
  },
  "timestamp": "2025-01-15T12:00:00.000Z"
}
```

**Success Response Format:**
```json
{
  "success": true,
  "data": [
    {
      "product_id": 1,
      "product_name": "Example Product",
      "price": 99.99
    }
  ],
  "rows": 1,
  "execution_time_ms": 15,
  "timestamp": "2025-01-15T12:00:00.000Z"
}
```

## Performance Notes

- The demo loads the entire products.csv dataset into memory for optimal query performance
- Query execution times are typically under 50ms for filtered queries
- The pipeline includes execution time metrics in the response
- Consider the `limit` parameter to control response size for large result sets

## Stopping the Server

Use `Ctrl+C` to gracefully shutdown the server.

## Troubleshooting

1. **"Failed to load server configuration"**: Verify file paths and ensure products.csv exists at `data/products.csv`
2. **"Connection refused"**: Check that the server started successfully and is listening on the correct port
3. **"Empty response"**: Check that your query parameters match available data in the dataset

## Other Data Source Demos

- [Lance Vector Search](lance/LANCE_DEMO.md) - KNN similarity search with Lance
- [ONNX Predict](onnx_predict/ONNX_PREDICT_DEMO.md) - ONNX model inference in SQL
- [PostgreSQL](postgres/POSTGRES_DEMO.md) - CRUD operations and federated queries
- [MySQL](mysql/MYSQL_DEMO.md) - CRUD operations and federated queries
- [MongoDB](mongo/MONGO_DEMO.md) - Document CRUD and federated queries
- [Apache Iceberg](iceberg/ICEBERG_DEMO.md) - Iceberg table queries
- [S3 Remote Files](S3_USAGE.md) - S3 data source configuration
