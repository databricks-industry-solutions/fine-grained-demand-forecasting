#!/bin/bash

# Fine-Grained Demand Forecasting - Cleanup Script
# This script removes all deployed Databricks resources

set -e

echo "🧹 Starting cleanup of Fine-Grained Demand Forecasting resources..."

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "❌ Error: databricks CLI is not installed"
    echo "Please install it with: pip install databricks-cli"
    exit 1
fi

# Validate bundle configuration
echo "📋 Validating bundle configuration..."
databricks bundle validate

# Destroy all resources
echo "🗑️  Destroying deployed resources..."
databricks bundle destroy --auto-approve

echo "✅ Cleanup completed successfully!"
echo ""
echo "📝 Summary:"
echo "  - All deployed jobs and notebooks have been removed"
echo "  - Unity Catalog tables remain (manual cleanup required if needed)"
echo "  - To clean up Unity Catalog resources manually:"
echo "    DROP SCHEMA IF EXISTS <catalog_name>.<schema_name> CASCADE;"
echo "    DROP CATALOG IF EXISTS <catalog_name> CASCADE;" 