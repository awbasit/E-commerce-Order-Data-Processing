
# Big Data Pipeline Project – E-commerce Order Data Processing

## Project Overview

This project is a **serverless, event-driven data pipeline** designed to automate the **real-time processing of e-commerce order data** using AWS services. It ingests order-related CSV files from an S3 bucket, validates them, transforms the data into analytical format, and stores the output into a database or another structured storage system for insights and analytics.

The solution uses **AWS Step Functions**, **ECS Fargate**, **Lambda**, and **SNS** to ensure scalability, automation, and failure notifications, helping businesses unlock timely and accurate insights from their order data.

---

## Business Purpose & Insights

### Why This Project Matters

- Modern e-commerce companies generate **thousands of transactions daily**.
- Raw transactional data is **not analytics-ready** and needs structured transformation.
- Businesses need timely insights on:
  - Sales volume and trends
  - Customer purchasing behavior
  - Top-selling products
  - Inventory movement
  - Order fulfillment patterns

### Business Insights Delivered

After processing, this pipeline can power dashboards and reports that provide:

- **Daily Order Volume** per region or product category
- **Product Performance**: top-selling and underperforming SKUs
- **Customer Order Patterns**: frequent buyers, order frequency
- **Revenue Analytics**: gross and net revenue by product/region

---

## Data Insights

### Input Data (CSV files in S3)

- `orders/`: Order transactions
- `order_items/`: Product items in each order
- `products/`: Product metadata

### Sample Order Data

| order_id | customer_id | order_date  | status     |
|----------|-------------|-------------|------------|
| 12345    | 456         | 2025-07-01  | completed  |

### Sample Order Items

| order_id | product_id | quantity | price  |
|----------|------------|----------|--------|
| 12345    | P1001      | 2        | 39.99  |

---

## Data Schema

### Orders Table
- `order_id` (PK)
- `customer_id`
- `order_date`
- `status`

### Order_Items Table
- `order_item_id` (PK)
- `order_id` (FK)
- `product_id` (FK)
- `quantity`
- `price`

### Products Table
- `product_id` (PK)
- `name`
- `category`
- `price`

---

## Data Transformations Implemented

- **Deduplication**: Remove duplicate records
- **Type conversion**: Convert dates, floats, and integers appropriately
- **Joining**: Orders + Order Items + Products
- **Aggregation**: Total revenue, average order value, product counts
- **Enrichment**: Add computed fields like `total_price`, `order_month`

---

## Entity Relationship Diagram (ERD)

Orders --< Order_Items >-- Products

---

## Project Workflow

1. **Files uploaded to S3** (triggered manually or via business app)
2. **Step Function triggered** when file threshold is reached
3. **Validation ECS task** checks file quality, format, and schema
4. **Transformation ECS task** cleans and aggregates the data
5. **On success**:
   - Data is saved to S3 or database
   - Notification is sent via SNS
   - Cleanup Lambda clears batch state
6. **On failure**:
   - Failure alert sent via SNS
   - Cleanup still executed

---

## Architecture Flow

S3 -> Step Functions -> ECS (Validation & Transformation) -> SNS + Lambda Cleanup

---

## Setup Instructions

### Prerequisites

- AWS account with access to:
  - IAM, S3, ECS, Lambda, Step Functions, SNS
- AWS CLI or Console
- Docker (for ECS images)

### 1. **Create and Configure Resources**

- Create **S3 bucket**: `project6dt/raw_data/`
- Create ECS clusters and tasks
- Setup **Step Function**, **SNS**, and **Lambda**

### 2. **IAM Role Permissions**

Include:

- `ecs:RunTask`, `ecs:DescribeTasks`
- `lambda:InvokeFunction`
- `sns:Publish`
- `dynamodb:UpdateItem`
- `iam:PassRole`

### 3. **ECS Container Deployment**

```bash
docker build -t validator .
docker tag validator <ecr-url>
docker push <ecr-url>
```

### 4. **Lambda Cleanup Input**

```json
{
  "executionArn": "<ARN>",
  "status": "SUCCESS | VALIDATION_FAILED | TRANSFORMATION_FAILED",
  "stepFunctionArn": "<StateMachine ARN>"
}
```

---

## Project Structure

```bash
project/
├── validator/
├── transformer/
├── lambda/
├── step_function/
└── README.md
```

---

## Summary

This solution demonstrates a full-scale, cloud-native pipeline for processing structured e-commerce data at scale, suitable for further enhancement with ML or BI tools.
