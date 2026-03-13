# Analytics Engineering Exam – Solution


## How to Reproduce

```bash
Python version 3.12
# 1. Install dependencies and create venv using uv
uv sync

# 2. change directory
cd lmwn_ae_exam

# 3. run DBT debug for check
dbt debug
```
### If you did't use uv please instll via pip
```bash
Python version 3.12
# 1.create venv using
python -m venv .venv

# 2. Activate the virtual environment
.\.venv\Scripts\Activate

# 3.Install packages from requirements.txt
pip install -r requirements.txt

# 4. change directory
cd lmwn_ae_exam

# 5. run DBT debug for check
dbt debug

# 5. run DBT debug for check
dbt build
```

## Approach

I used **dbt** with the `dbt-duckdb` adapter to build a **3-layer data model**:

1. **Staging (`model_stg_*`)** — Clean and rename source tables, add derived columns (e.g., delivery duration, resolution time)
2. **Intermediate (`model_int_*`)** — Join and enrich across entities (orders + customers + drivers + restaurants), pre-aggregate for reporting
3. **Reports (`report_*`)** — Final aggregated reports matching business requirements

## Data Model

| Layer |
|-------|
| Staging |
| Intermediate |
| Reports |

## Reports Produced

### Performance Marketing Team
| Report | Table Name |
|--------|-----------|
| Campaign Effectiveness | `report_campaign_effectiveness` |
| Customer Acquisition | `report_customer_acquisition` |
| Retargeting Performance | `report_retargeting_performance` |

### Fleet Management Team
| Report | Table Name |
|--------|-----------|
| Driver Performance | `report_driver_performance` |
| Delivery Zone Heatmap | `report_delivery_zone_heatmap` |
| Driver Incentive Impact | `report_driver_incentive_impact` |

### Customer Service Team
| Report | Table Name |
|--------|-----------|
| Complaint Summary | `report_complaint_summary` |
| Driver-Related Complaints | `report_driver_complaints` |
| Restaurant Quality | `report_restaurant_quality` |

## Testing

(unique + not_null on primary keys for all staging models).

## ERD

See [Lineage_Diagram](Lineage_Daigram.png) for the data lineage diagram.

How to generate Lineage Diagram

```bash
dbt docs generate
dbt docs serve
```