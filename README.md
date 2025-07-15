# Fine-Grained Demand Forecasting 📈

[![Deploy](https://github.com/user/fine-grained-demand-forecasting/actions/workflows/deploy.yml/badge.svg)](https://github.com/user/fine-grained-demand-forecasting/actions/workflows/deploy.yml)
[![Template](https://img.shields.io/badge/template-industry--solutions--blueprints-blue)](https://github.com/databricks-industry-solutions/industry-solutions-blueprints)

A scalable demand forecasting solution built on Databricks using Facebook Prophet, Unity Catalog, and serverless compute. This solution demonstrates modern MLOps practices for retail and supply chain forecasting at the store-item level.

**✨ 2025 Modern Implementation** - Fully compliant with [Databricks Industry Solutions Blueprints](https://github.com/databricks-industry-solutions/industry-solutions-blueprints) template.

## 🚀 Quick Start

1. **Prerequisites**
   ```bash
   pip install databricks-cli
   ```

2. **Configure Databricks**
   ```bash
   # Option A: Interactive configuration
   databricks configure
   
   # Option B: Environment file (recommended)
   cp env.example .env
   # Edit .env with your Databricks workspace URL, token, and warehouse ID
   ```

3. **Deploy Everything**
   ```bash
   ./scripts/deploy.sh
   ```

4. **Clean Up When Done**
   ```bash
   ./scripts/cleanup.sh
   ```

## 📊 What Gets Deployed

- **Workflow**: `Fine-Grained Demand Forecasting Pipeline`
- **Notebooks**: `demand_forecasting_pipeline.ipynb` (Unity Catalog + Prophet forecasting)
- **Dashboard**: `Fine-Grained Demand Forecasting Dashboard` (real-time insights)
- **App**: `demand-forecasting-app` (Streamlit interactive explorer)
- **Location**: `/Workspace/Users/your-email@company.com/fine-grained-demand-forecasting-dev/`

## 🔧 Manual Commands

```bash
databricks bundle validate          # Check configuration
databricks bundle deploy            # Deploy to workspace
databricks bundle run demand_forecasting_workflow  # Run forecasting
databricks bundle summary           # See what's deployed
databricks bundle destroy           # Remove everything
```

## 🏗️ Project Structure

```
├── databricks.yml                 # Main DABs configuration
├── notebooks/
│   └── demand_forecasting_pipeline.ipynb  # Main forecasting notebook
├── dashboards/
│   └── demand_forecasting_dashboard.lvdash.json  # Real-time dashboard
├── apps/
│   └── demand_app/
│       ├── app.py                 # Streamlit forecasting app
│       └── app.yaml               # App configuration
├── src/
│   └── demand_forecasting/        # Python package for forecasting logic
│       ├── __init__.py
│       ├── data_generation.py     # Synthetic data generation
│       └── forecasting.py         # Prophet-based forecasting
├── scripts/
│   ├── deploy.sh                  # Automated deployment
│   └── cleanup.sh                 # Automated cleanup
├── .github/workflows/
│   └── deploy.yml                 # CI/CD pipeline
├── requirements.txt               # Python dependencies
└── env.example                    # Environment configuration template
```

## ✨ Key Features

### 🎯 Modern Databricks Architecture
- **Asset Bundle (DAB) Deployment** - Infrastructure as code with multi-environment support
- **Unity Catalog Integration** - Enterprise data governance and lineage
- **Serverless Compute** - Cost-efficient auto-scaling with SQL warehouses and Photon engine
- **100% Python Implementation** - Eliminated legacy R dependencies

### 📈 Advanced Forecasting
- **Facebook Prophet Models** - Robust time series forecasting with seasonality detection
- **Distributed Processing** - Pandas UDFs for scalable store-item level forecasting
- **Synthetic Data Generation** - No external data dependencies (replaces Kaggle)
- **Confidence Intervals** - Prediction uncertainty quantification

### 🔄 MLOps Best Practices
- **CI/CD Pipeline** - Automated testing and multi-stage deployment (dev → staging → prod)
- **Data Quality Validation** - Automated checks for forecasting readiness
- **Model Versioning** - Tracked model artifacts and performance metrics
- **Real-time Dashboards** - Interactive Lakeview dashboards for business users

### 🛡️ Enterprise Ready
- **Role-Based Access Control (RBAC)** - Unity Catalog security integration
- **Multi-Environment Support** - Development, staging, and production configurations
- **Audit Logging** - Complete data lineage and governance tracking
- **Serverless Cost Optimization** - Pay-per-use compute with automatic scaling

## 🎛️ Configuration

### Environment Variables (.env)
```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com/
DATABRICKS_TOKEN=your-access-token
DATABRICKS_WAREHOUSE_ID=your-warehouse-id
CATALOG_NAME=dev_demand_forecasting
SCHEMA_NAME=forecasting
```

### Databricks Bundle Targets
- **dev**: Development environment (single-user, personal workspace)
- **staging**: Staging environment (shared workspace, validation)
- **prod**: Production environment (service principal, enterprise governance)

## 📊 Forecasting Pipeline

1. **Data Generation**: Synthetic sales data with realistic seasonal patterns, trends, and noise
2. **Unity Catalog Setup**: Automated catalog, schema, and table creation with optimizations
3. **Quality Validation**: Data completeness and forecasting readiness checks
4. **Distributed Forecasting**: Prophet models across store-item combinations using Pandas UDFs
5. **Results Storage**: Forecast results with confidence intervals stored in Delta tables
6. **Visualization**: Interactive dashboards and Streamlit apps for forecast exploration

## 🎨 Dashboard & Analytics

The solution includes comprehensive visualization components:

- **Lakeview Dashboard**: Real-time forecast summaries, trends, and accuracy metrics
- **Streamlit App**: Interactive forecast explorer with filtering and drill-down capabilities
- **Plotly Visualizations**: Time series plots with confidence intervals and seasonality decomposition

## 🔄 CI/CD Pipeline

Automated GitHub Actions workflow:
- **Pull Requests**: Validation and testing with isolated workspace paths
- **Main Branch**: Deployment to development environment
- **Production**: Scheduled or manual deployment with approval gates
- **Cleanup**: Automatic resource cleanup when PRs are closed

## 🏪 Use Cases

- **Retail Demand Planning**: Store-level inventory optimization
- **Supply Chain Forecasting**: Multi-location demand coordination
- **Revenue Forecasting**: Financial planning and budgeting
- **Capacity Planning**: Resource allocation and workforce planning

## 🔗 Template Compliance

This solution is fully compliant with the [Databricks Industry Solutions Blueprints](https://github.com/databricks-industry-solutions/industry-solutions-blueprints) template, ensuring:

- ✅ Standard DAB structure and configuration
- ✅ Jupyter notebook format (.ipynb)
- ✅ Dashboard and app deployment
- ✅ Automated deployment and cleanup scripts
- ✅ Environment configuration templates
- ✅ CI/CD pipeline integration

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with `databricks bundle validate`
5. Submit a pull request

## 📜 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For issues and questions:
- Check the [GitHub Issues](../../issues)
- Review [Databricks Documentation](https://docs.databricks.com/)
- Consult [Asset Bundle Guide](https://docs.databricks.com/dev-tools/bundles/index.html)

---

**Built with ❤️ using Databricks Asset Bundles, Unity Catalog, and Prophet** | *Modernized for 2025*
