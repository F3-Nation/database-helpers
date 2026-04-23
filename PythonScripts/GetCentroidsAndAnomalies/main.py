#!/usr/bin/env python3

import os
import psycopg2
from psycopg2.extras import RealDictCursor
import statistics
from geopy.distance import geodesic
from dotenv import load_dotenv
import csv


############################################
# Load .env file
############################################
load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "5432")),
}


############################################
# Utility functions
############################################

def is_bad_coordinate(lat, lon):
    """Detect common bad data entry problems."""
    if lat is None or lon is None:
        return True
    if not (-90 <= lat <= 90):
        return True
    if not (-180 <= lon <= 180):
        return True
    if lat == 0 and lon == 0:
        return True
    return False


############################################
# Fetch all data using your SQL
############################################

def load_locations_with_regions():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("""
        SELECT
            l.id AS location_id,
            l.name AS location_name,
            l.latitude,
            l.longitude,
            region.id AS region_id,
            region.name AS region_name
        FROM locations l
        LEFT JOIN orgs region ON region.id = l.org_id
        where l.is_active 
    """)
    rows = cursor.fetchall()

    conn.close()
    return rows


############################################
# Build region → list of locations structure
############################################

def group_by_region(rows):
    region_map = {}

    for r in rows:
        region_id = r["region_id"]
        if region_id is None:
            continue

        region_map.setdefault(region_id, {
            "region_id": region_id,
            "region_name": r["region_name"],
            "locations": []
        })

        region_map[region_id]["locations"].append({
            "location_id": r["location_id"],
            "location_name": r["location_name"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
        })

    return region_map


############################################
# Analyze locations inside a region
############################################

def analyze_region(region_name, locs):
    anomalies = []

    # Separate valid and invalid coordinates
    valid_locs = []
    for loc in locs:
        if is_bad_coordinate(loc["latitude"], loc["longitude"]):
            anomalies.append({
                "type": "invalid_coordinate",
                "message": (
                    f"{loc['location_name']} has invalid coordinates "
                    f"({loc['latitude']}, {loc['longitude']})"
                ),
                "location": loc,
            })
        else:
            valid_locs.append(loc)

    if len(valid_locs) < 2:
        return anomalies  # Not enough valid points for spread analysis

    # Compute centroid only on valid coordinates
    avg_lat = sum(l["latitude"] for l in valid_locs) / len(valid_locs)
    avg_lon = sum(l["longitude"] for l in valid_locs) / len(valid_locs)

    distances = []
    for loc in valid_locs:
        d = geodesic((avg_lat, avg_lon), (loc["latitude"], loc["longitude"])).miles
        loc["distance_from_centroid"] = d
        distances.append(d)

    mean_d = statistics.mean(distances)
    sd_d = statistics.stdev(distances) if len(distances) > 1 else 0
    z_threshold = 3.0

    for loc in valid_locs:
        d = loc["distance_from_centroid"]
        if sd_d > 0:
            z = abs((d - mean_d) / sd_d)
            if z > z_threshold:
                anomalies.append({
                    "type": "location_spread_outlier",
                    "message": (
                        f"{loc['location_name']} is {d:.1f} miles from centroid (z={z:.2f})"
                    ),
                    "location": loc,
                })
        if d > 300:
            anomalies.append({
                "type": "extreme_distance",
                "message": (
                    f"{loc['location_name']} is unrealistically far ({d:.1f} miles)"
                ),
                "location": loc,
            })

    return anomalies


############################################
# Main execution
############################################

def run():
    print("Loading data...")
    rows = load_locations_with_regions()
    region_map = group_by_region(rows)

    all_anomalies = []
    centroids = []

    print("\nAnalyzing regions...\n")

    for region_id, data in region_map.items():
        region_name = data["region_name"]
        locs = data["locations"]

        print(f"Region: {region_name} ({len(locs)} locations)")

        # Separate valid coordinates for centroid computation
        valid_locs = [l for l in locs if not is_bad_coordinate(l["latitude"], l["longitude"])]

        if valid_locs:
            avg_lat = sum(l["latitude"] for l in valid_locs) / len(valid_locs)
            avg_lon = sum(l["longitude"] for l in valid_locs) / len(valid_locs)
            centroids.append({
                "region_name": region_name,
                "centroid_lat": avg_lat,
                "centroid_lon": avg_lon
            })
        else:
            # If no valid locations, set centroid as None
            avg_lat = avg_lon = None
            centroids.append({
                "region_name": region_name,
                "centroid_lat": None,
                "centroid_lon": None
            })

        # Analyze region for anomalies
        anomalies = analyze_region(region_name, locs)
        if anomalies:
            print(f"  → Found {len(anomalies)} anomalies")
        for a in anomalies:
            all_anomalies.append({
                "region_name": region_name,
                "location_name": a["location"]["location_name"],
                "anomaly_reason": a["type"] if a["type"] != "invalid_coordinate" else "invalid lat/long"
            })

    # Write anomalies CSV
    with open("location_anomalies.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["region_name","location_name","anomaly_reason"])
        writer.writeheader()
        for row in all_anomalies:
            writer.writerow(row)

    print("Wrote location_anomalies.csv")

    # Write centroids CSV
    with open("region_centroids.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["region_name","centroid_lat","centroid_lon"])
        writer.writeheader()
        for row in centroids:
            writer.writerow(row)

    print("Wrote region_centroids.csv")


if __name__ == "__main__":
    run()
