
# DataLake API

A Django REST API for accessing data from Parquet files and MariaDB tables.

## API Endpoints

### Data Sources
- `/api/transactions/` - List all available data sources
- `/api/transactions/parquet/{folder_name}/` - Access data from a specific Parquet folder
- `/api/transactions/db/{table_name}/` - Access data from a specific MariaDB table

### Metrics
- `/api/transactions/metrics/recent-spending/` - Get money spent in the last 5 minutes
  - Optional parameter: `minutes` (default: 5) - Change the time window
- `/api/transactions/metrics/user-spending/` - Get total spent per user and transaction type
- `/api/transactions/metrics/top-products/` - Get the top X products bought
  - Optional parameter: `limit` (default: 10) - Change the number of products returned

## Filtering

All data endpoints support the following filters:

### Basic Filters

- `payment_method`: Filter by payment method (e.g., `?payment_method=paypal`)
- `country`: Filter by country (e.g., `?country=USA`)
- `product_category`: Filter by product category (e.g., `?product_category=electronics`)
- `status`: Filter by transaction status (e.g., `?status=completed`)

### Amount Filters

- `amount_gt`: Filter by amount greater than (e.g., `?amount_gt=100`)
- `amount_lt`: Filter by amount less than (e.g., `?amount_lt=500`)
- `amount_eq`: Filter by exact amount (e.g., `?amount_eq=299.99`)

### Customer Rating Filters

- `rating_gt`: Filter by rating greater than (e.g., `?rating_gt=3`)
- `rating_lt`: Filter by rating less than (e.g., `?rating_lt=5`)
- `rating_eq`: Filter by exact rating (e.g., `?rating_eq=4`)

### Multiple Filters

Filters can be combined (e.g., `?payment_method=paypal&country=USA&amount_gt=100`).

For list type filters (payment_method, country, product_category, status), multiple values can be specified by repeating the parameter:
`?payment_method=paypal&payment_method=credit_card`

## Authentication

All endpoints require authentication using token-based authentication.

- `/api/transactions/auth/login/` - Obtain an authentication token

## Permissions

- `/api/transactions/permissions/grant/` - Grant table access permissions
- `/api/transactions/permissions/revoke/` - Revoke table access permissions
- `/api/transactions/permissions/list/` - List user permissions