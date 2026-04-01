import pandas as pd
import sqlite3

print("Starting analysis...\n")

# Load data
data = pd.read_csv("food_safety_data.csv")

# Fix column names
data.columns = data.columns.str.strip().str.replace(" ", "_")

# -------------------------------
# 🔥 RISK SCORE FUNCTION
# -------------------------------
def calculate_risk(row):
    score = 0
    
    if row["Temperature"] >= 30:
        score += 30
    elif row["Temperature"] >= 20:
        score += 20
    else:
        score += 10

    if row["Storage_Time"] >= 6:
        score += 30
    elif row["Storage_Time"] >= 4:
        score += 20
    else:
        score += 10

    if row["Bacterial_Count"] >= 1500:
        score += 40
    elif row["Bacterial_Count"] >= 800:
        score += 25
    else:
        score += 10

    return score

# Apply Risk Score
data["Risk_Score"] = data.apply(calculate_risk, axis=1)

# -------------------------------
# 🔥 DECISION SYSTEM
# -------------------------------
def classify_decision(score):
    if score > 80:
        return "Unsafe"
    elif score >= 50:
        return "Monitor"
    else:
        return "Safe"

data["Decision"] = data["Risk_Score"].apply(classify_decision)

# -------------------------------
# SAVE FINAL DATASET
# -------------------------------
data.to_csv("final_food_safety.csv", index=False)

print("Final dataset created successfully!\n")

# -------------------------------
# SQL PART
# -------------------------------
conn = sqlite3.connect("food.db")
data.to_sql("food", conn, if_exists="replace", index=False)

query = """
SELECT Risk_Level, COUNT(*) as Count
FROM food
GROUP BY Risk_Level
"""

print("Risk Distribution:\n")
print(pd.read_sql(query, conn))

conn.close()

print("\nDone!")