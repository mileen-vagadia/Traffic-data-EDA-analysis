# traffic-eda

EDA on urban traffic data using Python and PostgreSQL.

I work on traffic control systems at my job so wanted to see how the data side of things looks — specifically around peak hours and whether signal timing actually affects wait times.

dataset: [Smart Traffic Management Dataset](https://www.kaggle.com/datasets/smmmmmmmmmmmm/smart-traffic-management-dataset) (Kaggle)

---

### what's in here

```
traffic-eda/
├── data/
│   └── raw/               # put the kaggle csv here
├── notebooks/
│   └── 01_eda.ipynb       # main analysis
├── sql/
│   ├── schema.sql
│   └── queries.sql
├── visuals/               # charts saved here
├── load_data.py
├── requirements.txt
└── .env.example
```

---

### how to run

```bash
# 1. clone and install
git clone https://github.com/mileen-vagadia/Traffic-data-EDA-analysis
cd Traffic-data-EDA-analysis
pip install -r requirements.txt

# 2. set up env
cp .env.example .env
# fill in your postgres credentials

# 3. create db and run schema
psql -U postgres -c "CREATE DATABASE traffic_db;"
psql -U postgres -d traffic_db -f sql/schema.sql

# 4. download the kaggle dataset, put csv in data/raw/traffic_data.csv
# then load it
python load_data.py

# 5. open the notebook
jupyter notebook notebooks/01_eda.ipynb
```

---

### analysis

two main things explored:

**peak hour analysis** — when does traffic actually peak, weekday vs weekend patterns, heatmap by hour and day

**signal timing vs wait time** — scatter of green duration vs wait time, bucketed analysis, wait by signal phase

findings are in the last cell of the notebook (updated after running).

---
