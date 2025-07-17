# Fine-Grained Demand Forecasting 📈

[![Databricks](https://img.shields.io/badge/Databricks-Solution_Accelerator-FF3621?style=for-the-badge&logo=databricks)](https://databricks.com)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00A1C9?style=for-the-badge)](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
[![Serverless](https://img.shields.io/badge/Serverless-Compute-00C851?style=for-the-badge)](https://docs.databricks.com/en/compute/serverless.html)

A scalable demand forecasting solution built on Databricks using Facebook Prophet, Unity Catalog, and serverless compute. This solution demonstrates modern MLOps practices for retail and supply chain forecasting at the store-item level.

## 🏪 Industry Use Case

**Fine-grained demand forecasting** represents a paradigm shift from traditional aggregate forecasting approaches. Instead of predicting demand at a high level (e.g., total company sales), fine-grained forecasting generates predictions for specific combinations of dimensions—in this case, **store-item level forecasting**.

### Why Fine-Grained Forecasting Matters

Traditional forecasting approaches often aggregate demand across locations, products, or time periods, losing critical nuances:

- **Aggregate Approach**: "We'll sell 10,000 units of Product A this month"
- **Fine-Grained Approach**: "Store 1 will sell 45 units of Product A, Store 2 will sell 67 units, Store 3 will sell 23 units..."

  
<img width="1038" height="512" alt="Demand_Forecasting_Plotly" src="https://github.com/user-attachments/assets/50877df7-fdc1-4de2-a452-e4e9ac4193fb" />

This granular approach addresses real-world business challenges:

- **Inventory Optimization**: Precise allocation of inventory across locations based on local demand patterns
- **Supply Chain Efficiency**: Targeted procurement and distribution strategies for each store-product combination
- **Revenue Protection**: Early identification of demand shifts at specific locations before they impact overall performance
- **Cost Reduction**: Elimination of safety stock inefficiencies caused by demand aggregation

### An Open-Source Approach to Complex Forecasting

This solution serves as **one inspirational approach** to tackle the technical challenges of fine-grained demand forecasting. The retail industry faces this problem universally, but solutions vary widely based on:

- **Scale Requirements**: From hundreds to millions of store-item combinations
- **Data Architecture**: Different approaches to distributed processing and storage
- **Algorithm Choice**: Prophet, ARIMA, neural networks, or hybrid approaches
- **Infrastructure**: Cloud-native vs. on-premises, serverless vs. traditional compute

**This implementation demonstrates:**
- How to structure a scalable forecasting pipeline using modern data platforms
- Practical approaches to distributed time series modeling
- Real-world considerations for data governance and MLOps

Whether you're a data scientist exploring forecasting techniques, a business leader understanding AI applications, or an engineer architecting similar solutions, this open-source example provides a foundation to build upon and adapt to your specific needs.

This solution scales from hundreds to thousands of store-item combinations, making it suitable for enterprise retail operations, e-commerce platforms, and multi-location businesses seeking to implement their own fine-grained forecasting capabilities.

## 🚀 Installation


### Recommended: Using Databricks Asset Bundle Editor

1. **Clone this repository** to your Databricks workspace:
   ```bash
   git clone https://github.com/databricks-industry-solutions/fine-grained-demand-forecasting.git
   ```

2. **Open the DAB Editor UI** in your Databricks workspace:
   - Navigate to the cloned repository folder
   - Open the `databricks.yml` file
   - Click "Edit Bundle" to open the visual editor

3. **Configure and Run** the bundle:
   - Modify configuration variables as needed (catalog name, schema name, environment)
   - Click "Validate" to check your configuration
   - Click "Deploy" to deploy all resources
   - Click "Run" to execute the demand forecasting workflow

### Alternative: Command Line

If you prefer using the command line:

```bash
# Prerequisites
pip install databricks-cli

# Configure Databricks
databricks configure

# Deploy and run
databricks bundle validate
databricks bundle deploy
databricks bundle run demand_forecasting_workflow
```

## 🏗️ Project Structure

```
├── databricks.yml                 # Main DABs configuration
├── notebooks/
│   ├── 01_data_generation_setup.py      # Data foundation and Unity Catalog setup
│   ├── 02_model_training_forecasting.py # Prophet model training and forecasting
│   └── 03_results_analysis_visualization.py # Business insights and visualization
├── .github/workflows/
│   ├── databricks-ci.yml         # CI/CD pipeline
│   └── publish.yaml              # Publishing workflow
├── scripts/                      # Deployment and utility scripts
├── requirements.txt              # Python dependencies
├── env.example                   # Environment configuration template
└── CONTRIBUTING.md               # Contribution guidelines
```

## 📊 Forecasting Pipeline

The solution implements a three-stage forecasting pipeline:

### 1. Data Generation & Setup (`01_data_generation_setup.py`)
- Synthetic sales data generation with realistic seasonal patterns
- Unity Catalog infrastructure setup (catalog, schema, tables)
- Data quality validation and governance setup

### 2. Model Training & Forecasting (`02_model_training_forecasting.py`)
- Facebook Prophet model training for each store-item combination
- Distributed processing using Pandas UDFs for scalability
- Confidence interval generation for uncertainty quantification
- Forecast results storage in Delta tables

### 3. Results Analysis & Visualization (`03_results_analysis_visualization.py`)
- Business insights and forecast accuracy metrics
- Interactive visualizations and trend analysis
- Executive dashboards and reporting

## 🔧 Configuration

### Environment Variables (.env)
```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com/
DATABRICKS_TOKEN=your-access-token
DATABRICKS_WAREHOUSE_ID=your-warehouse-id
CATALOG_NAME=dev_demand_forecasting
SCHEMA_NAME=forecasting
```

### Key Configuration Options
- **Catalog Name**: Unity Catalog name for data governance
- **Schema Name**: Database schema for forecasting tables
- **Environment**: Deployment environment (dev/staging/prod)
- **Forecast Horizon**: Number of days to forecast ahead (configurable)

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

**Built with ❤️ using Databricks Asset Bundles, Unity Catalog, and Prophet**
