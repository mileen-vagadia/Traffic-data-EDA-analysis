import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

df = pd.read_sql("select * from traffic_observations", engine, parse_dates=["timestamp"])
print(df.shape)
print(df.isnull().sum())
print(df.describe())

# basic feature extraction
df["hour"] = df["timestamp"].dt.hour
df["day_name"] = df["timestamp"].dt.day_name()
df["day_num"] = df["timestamp"].dt.dayofweek
df["is_weekend"] = df["day_num"].isin([5, 6])

def tod(h):
    if 6 <= h < 10: return "morning peak"
    if 10 <= h < 17: return "midday"
    if 17 <= h < 21: return "evening peak"
    return "night"

df["time_of_day"] = df["hour"].apply(tod)

# -- peak hour analysis --

hourly = df.groupby("hour")["vehicle_count"].mean().reset_index()
print(hourly.sort_values("vehicle_count", ascending=False).head(5))

fig, ax = plt.subplots(figsize=(12, 4))
ax.bar(hourly["hour"], hourly["vehicle_count"], color="steelblue", width=0.7)
ax.set_xlabel("hour")
ax.set_ylabel("avg vehicle count")
ax.set_title("traffic by hour")
ax.set_xticks(range(0, 24))
plt.tight_layout()
plt.savefig("visuals/peak_hour.png")
plt.show()

# weekday vs weekend
wd = df.groupby(["hour", "is_weekend"])["vehicle_count"].mean().reset_index()
wd["label"] = wd["is_weekend"].map({False: "weekday", True: "weekend"})

fig, ax = plt.subplots(figsize=(12, 4))
for label, grp in wd.groupby("label"):
    ax.plot(grp["hour"], grp["vehicle_count"], marker="o", label=label)
ax.set_xlabel("hour")
ax.set_ylabel("avg vehicle count")
ax.set_title("weekday vs weekend")
ax.legend()
plt.tight_layout()
plt.savefig("visuals/weekday_weekend.png")
plt.show()

# heatmap
day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
pivot = df.groupby(["day_name", "hour"])["vehicle_count"].mean().unstack()
pivot = pivot.reindex(day_order)

fig, ax = plt.subplots(figsize=(14, 5))
sns.heatmap(pivot, cmap="YlOrRd", linewidths=0.2, ax=ax)
ax.set_title("traffic heatmap")
plt.tight_layout()
plt.savefig("visuals/heatmap.png")
plt.show()

# -- signal timing vs wait time --

sig = df.dropna(subset=["green_duration_sec", "wait_time_sec"]).copy()

corr = sig["green_duration_sec"].corr(sig["wait_time_sec"])
print("green vs wait correlation:", round(corr, 3))

fig, ax = plt.subplots(figsize=(8, 5))
scatter = ax.scatter(
    sig["green_duration_sec"],
    sig["wait_time_sec"],
    alpha=0.25,
    c=sig["vehicle_count"],
    cmap="coolwarm",
    s=12
)
plt.colorbar(scatter, ax=ax, label="vehicle count")
z = np.polyfit(sig["green_duration_sec"], sig["wait_time_sec"], 1)
p = np.poly1d(z)
x_range = np.linspace(sig["green_duration_sec"].min(), sig["green_duration_sec"].max(), 200)
ax.plot(x_range, p(x_range), "k-", linewidth=1.5)
ax.set_xlabel("green duration (sec)")
ax.set_ylabel("wait time (sec)")
ax.set_title("green duration vs wait time")
plt.tight_layout()
plt.savefig("visuals/green_vs_wait.png")
plt.show()

sig["green_bucket"] = pd.cut(
    sig["green_duration_sec"],
    bins=[0, 30, 60, 90, 999],
    labels=["<30s", "30-60s", "60-90s", ">90s"]
)

bucket_agg = sig.groupby("green_bucket", observed=True).agg(
    avg_wait=("wait_time_sec", "mean"),
    avg_volume=("vehicle_count", "mean"),
    n=("id", "count")
).reset_index()

print(bucket_agg)

fig, ax1 = plt.subplots(figsize=(8, 4))
ax2 = ax1.twinx()
ax1.bar(bucket_agg["green_bucket"], bucket_agg["avg_wait"], color="steelblue", alpha=0.7)
ax2.plot(bucket_agg["green_bucket"], bucket_agg["avg_volume"], color="tomato", marker="o")
ax1.set_ylabel("avg wait (sec)", color="steelblue")
ax2.set_ylabel("avg vehicle count", color="tomato")
ax1.set_xlabel("green duration bucket")
ax1.set_title("wait time vs volume by green duration")
plt.tight_layout()
plt.savefig("visuals/bucket_analysis.png")
plt.show()

phase = df.groupby("signal_phase")[["wait_time_sec", "vehicle_count"]].mean().dropna()
print(phase)

colors = {"green": "#2ecc71", "yellow": "#f1c40f", "red": "#e74c3c"}
c = [colors.get(p, "gray") for p in phase.index]

fig, ax = plt.subplots(figsize=(6, 4))
ax.bar(phase.index, phase["wait_time_sec"], color=c)
ax.set_ylabel("avg wait time (sec)")
ax.set_title("wait time by phase")
plt.tight_layout()
plt.savefig("visuals/phase_wait.png")
plt.show()
