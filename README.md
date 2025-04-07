# Power BI Automation Agent

![Power BI Automation Agent Banner]([https://raw.githubusercontent.com/username/powerbi-automation-agent/main/ui_previews/powerbi_tab.png](https://github.com/KimiaHashemi/powerbi-automation-agent/blob/27c4c626422559e267d6f4f77075c764a4e6eb99/Screencastfrom04-07-2025090635PM-ezgif.com-video-to-gif-converter.gif))

A comprehensive Python-based desktop application that automates the entire data pipeline from SQL database extraction to Power BI reporting and SSRS deployment.

## Features

- **SQL Database Integration**
  - Connect to SQL Server, MySQL, PostgreSQL, Oracle, and SQLite databases
  - Execute custom SQL queries with preview functionality
  - Browse available tables and schemas

- **Data Transformation**
  - Remove duplicates from datasets
  - Fill missing values with smart strategies
  - Calculate revenue and business metrics
  - Generate time-based trends and analysis
  - Customer segmentation with RFM analysis

- **Power BI Integration**
  - Create and manage Power BI datasets
  - Generate reports from data or templates
  - Publish reports to Power BI workspaces
  - Preview data visualizations

- **SSRS Deployment**
  - Deploy reports to SQL Server Reporting Services
  - Schedule automated report generation
  - Export reports to multiple formats (PDF, Excel, Word, CSV, XML)

- **End-to-End Pipeline Automation**
  - Create reusable automation pipelines
  - Schedule pipeline execution
  - Monitor progress with detailed logs
  - Save and load pipeline configurations

## Screenshots

### Database Connection
![Database Connection](https://raw.githubusercontent.com/username/powerbi-automation-agent/main/ui_previews/connection_tab.png)

### Data Extraction
![Data Extraction](https://raw.githubusercontent.com/username/powerbi-automation-agent/main/ui_previews/data_tab.png)

### Data Transformation
![Data Transformation](https://raw.githubusercontent.com/username/powerbi-automation-agent/main/ui_previews/transform_tab.png)

### Power BI Integration
![Power BI Integration](https://raw.githubusercontent.com/username/powerbi-automation-agent/main/ui_previews/powerbi_tab.png)

### SSRS Deployment
![SSRS Deployment](https://raw.githubusercontent.com/username/powerbi-automation-agent/main/ui_previews/ssrs_tab.png)

### Pipeline Automation
![Pipeline Automation](https://github.com/KimiaHashemi/powerbi-automation-agent/blob/ec9ab0523236553aa46dfa1a99ea5d74fa1b4424/Screenshot%20from%202025-04-07%2021-13-41.png)

## Installation

### Prerequisites

- Python 3.7 or higher
- SQL Server, MySQL, PostgreSQL, Oracle, or SQLite database
- Power BI account (for Power BI integration)
- SQL Server Reporting Services instance (for SSRS deployment)

### Dependencies

Install the required Python packages:

```bash
pip install -r requirements.txt
```

The requirements.txt file contains:

```
pyodbc>=4.0.30
pandas>=1.3.0
matplotlib>=3.4.2
seaborn>=0.11.1
Pillow>=8.2.0
python-dateutil>=2.8.1
numpy>=1.20.0
```

### Optional Dependencies

For full functionality with Power BI:

```bash
pip install powerbiclient
```

## Usage

### Running the Application

```bash
python powerbi_agent.py
```

### Input Methods

The application accepts various inputs:

1. **SQL Connection**
   - Server name/address
   - Database name
   - Authentication credentials
   - Direct connection string

2. **Data Extraction**
   - SQL queries
   - Table selection

3. **Data Transformation**
   - Column selections for transformations
   - Parameters for metrics calculations
   - Time period selections

4. **Power BI Integration**
   - Workspace credentials
   - Dataset names and configurations
   - Report templates

5. **SSRS Deployment**
   - Server URL
   - Report paths
   - Schedule configurations
   - Export format selections

### Output Methods

The application produces:

1. **Transformed Data**
   - CSV exports of processed data
   - Visualizations and statistical summaries

2. **Power BI Assets**
   - Datasets in Power BI workspaces
   - Reports based on transformed data
   - Templates for future reporting

3. **SSRS Reports**
   - Deployed reports in SSRS
   - Scheduled report execution
   - Exported report files (PDF, Excel, etc.)

4. **Pipeline Configurations**
   - JSON configuration files for repeatable workflows
   - Execution logs

## Step-by-Step Example

1. **Connect to Database**
   - Enter SQL Server connection details
   - Test the connection
   - View available tables

2. **Extract Data**
   - Write a SQL query or select a table
   - Execute and preview the results
   - Validate data quality

3. **Transform Data**
   - Select appropriate transformations
   - Configure parameters
   - Preview transformed data and visualizations

4. **Create Power BI Dataset**
   - Authenticate with Power BI
   - Name and configure the dataset
   - Upload transformed data

5. **Generate Power BI Report**
   - Select a template or create from scratch
   - Configure report settings
   - Preview and publish

6. **Deploy to SSRS**
   - Connect to SSRS server
   - Set deployment path
   - Configure scheduling and export options

7. **Automate Pipeline**
   - Save configuration for future use
   - Schedule regular execution
   - Monitor results

## Code Structure

- `powerbi_agent.py` - Main application and UI
- `sql_extractor.py` - SQL database connection and query execution
- `data_transformer.py` - Data transformation and business logic
- `powerbi_integration.py` - Power BI API integration
- `ssrs_agent.py` - SSRS deployment and scheduling
- `pipeline_orchestrator.py` - End-to-end pipeline management
- `ui_preview_generator.py` - Generates UI previews (for documentation)

## Configuration

The application stores configurations in JSON format:

```json
{
  "sql_query": "SELECT * FROM Sales.SalesOrderHeader",
  "transformations": ["remove_duplicates", "calculate_revenue_metrics"],
  "dataset_name": "Sales_Analysis",
  "report_name": "Monthly_Sales_Report",
  "ssrs_path": "/Reports/Sales/MonthlyReport",
  "schedule": {
    "enabled": true,
    "frequency": "Weekly",
    "time": "02:00"
  }
}
```

## Extending the Application

### Adding Custom Transformations

1. Create a new transformation function in the `BusinessDataTransformer` class
2. Register the transformation in the `register_default_transformations` method
3. Add UI elements for the transformation parameters

Example:

```python
def calculate_customer_lifetime_value(self, data, purchase_col="TotalDue", 
                                     customer_col="CustomerID",
                                     date_col="OrderDate"):
    """Calculate customer lifetime value (CLV)"""
    df = data.copy()
    
    # Ensure date column is datetime
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Group by customer
    clv_data = df.groupby(customer_col).agg({
        purchase_col: 'sum',
        date_col: ['min', 'max', 'count']
    })
    
    # Calculate lifetime and frequency
    clv_data.columns = ['total_spend', 'first_purchase', 'last_purchase', 'purchase_count']
    clv_data['lifetime_days'] = (clv_data['last_purchase'] - clv_data['first_purchase']).dt.days
    clv_data['avg_purchase_value'] = clv_data['total_spend'] / clv_data['purchase_count']
    
    # Calculate CLV
    clv_data['customer_lifetime_value'] = clv_data['avg_purchase_value'] * clv_data['purchase_count']
    
    return clv_data
```

### Adding New Database Support

1. Modify the `SQLDataExtractor` class to handle the new database type
2. Add appropriate connection string formation in the UI
3. Test with the specific database drivers

### Integrating with Other BI Tools

The application architecture allows for adding modules for other BI tools:

1. Create a new agent class (similar to `PowerBIAgent` or `SSRSAgent`)
2. Implement authentication, dataset creation, and report generation
3. Add a new tab in the UI for the tool
4. Update the orchestrator to include the new tool in the pipeline

## Troubleshooting

### Common Issues

- **Database Connection Failures**
  - Verify network connectivity
  - Check credentials and permissions
  - Ensure database drivers are installed

- **Power BI Authentication Errors**
  - Verify workspace ID, client ID, and tenant ID
  - Check Power BI Pro subscription status
  - Ensure proper API permissions

- **SSRS Deployment Issues**
  - Verify SSRS is running and accessible
  - Check user permissions on the SSRS server
  - Validate report path exists

### Logs

The application maintains logs in the `logs` directory, with detailed information about:

- SQL queries executed
- Transformations applied
- API calls to Power BI and SSRS
- Pipeline execution details

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements

- Microsoft for Power BI and SSRS APIs
- Python ODBC community for database connectivity
- Pandas and Matplotlib for data processing and visualization
- Tkinter for the UI framework

---

Created by kimia Hashemi
