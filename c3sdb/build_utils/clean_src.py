"""
c3sdb/build_utils/clean_src.py

Ryan Nguyen (ryan97@uw.edu)

module for cleaning C3S database file
"""

import sqlite3
import numpy as np
import os

INCLUDE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "_include"
)


def calculate_rsd(values):
    """
    Calculates the relative standard deviation (RSD) for a list of values

    Parameters
    ----------
    values : ``list``
        list of values to calculate the RSD

    Returns
    -------
    ``float``
        RSD of the input values
    """
    return np.std(values) / np.mean(values) * 100


def remove_outliers_and_average(values):
    """
    Removes outliers from the list and averages the remaining values if their RSD is below 1%

    Parameters
    ----------
    values : ``list``
        list of numerical values from which to remove values

    Returns
    -------
    values : ``float`` or ``list``
        average of the values if RSD < 1% after removing outliers, or the original values otherwise
    """
    original_values = values[:]
    # continue process until there are two or fewer values left
    while len(values) > 2:
        mean_val = np.mean(values)
        std_dev = np.std(values)
        # remove values more than one standard deviation from the mean
        filtered_values = [v for v in values if abs(v - mean_val) <= std_dev]
        if len(filtered_values) == len(values):
            break
        values = filtered_values
        # if RSD < 1%, simply return the average
        if calculate_rsd(values) < 1:
            return np.mean(values)
        if len(values) != len(original_values):
            return np.mean(values)
        # otherwise return the original values
        else:
            return original_values


def process_entries(entries):
    """
    Processes entries to either average their CCS or leave them unchanged based on RSD and ccs_type

    Parameters
    ----------
    entries : ``list``
        list of entries containing CCS

    Returns
    -------
    ``list``
        processed CCS values for the entries
    """
    # extract CCS values
    ccs_values = [e["ccs"] for e in entries]
    # calculate RSD of CCS values
    rsd = calculate_rsd(ccs_values)
    # identify entries with DT type CCS measurements
    dt_entries = [e for e in entries if e["ccs_type"] == "DT"]
    # determine processing based on number of values and RSD
    # logic for handling duplicate entries (exactly two entries in group)
    if len(ccs_values) == 2:
        if rsd < 1:
            # if RSD < 1%, simply average the values
            return [round(np.mean(ccs_values), 4)] * len(entries)
        # if RSD > 1%, process based on ccs_type
        else:
            # Handling cases with exactly two entries and DT considerations
            if len(dt_entries) == 1:
                # If exactly one entry is DT and RSD > 1%, keep only the DT measurement
                dt_ccs = [e["ccs"] for e in dt_entries]
                return dt_ccs * len(entries)
            elif len(dt_entries) == 2:
                # If both are DT and RSD > 1%, keep both values
                return ccs_values
            else:
                # If no entries are DT and RSD > 1%, keep both values
                return ccs_values
    else:
        if rsd < 1:
            # If RSD < 1%, average all CCS values
            return [round(np.mean(ccs_values), 4)] * len(entries)
        else:
            # If RSD > 1%, attempt to remove outliers and recheck RSD
            new_values = remove_outliers_and_average(ccs_values)
            if isinstance(new_values, list):
                # If the result is still a list (outliers present), return individual values
                return new_values
            else:
                # If a single value is computed (no significant outliers), use this value for all entries
                return [round(new_values, 4)] * len(entries)


def create_clean_db(clean_db_path):
    """
    Creates the clean database using the schema files from the specified include path

    Parameters
    ----------
    clean_db_path : ``str``
        path to clean database, if it exists
    include_path : ``str``
        path to SQLite3 schema files
    """
    if os.path.exists(clean_db_path):
        os.remove(clean_db_path)
    con = sqlite3.connect(clean_db_path)
    cur = con.cursor()
    # point to correct SQLit3 schema scripts
    sql_scripts = [
        os.path.join(INCLUDE_PATH, "C3SDB_schema.sqlite3"),
        os.path.join(INCLUDE_PATH, "mqn_schema.sqlite3"),
        os.path.join(INCLUDE_PATH, "pred_CCS_schema.sqlite3"),
    ]
    for sql_script in sql_scripts:
        with open(sql_script, "r") as sql_file:
            cur.executescript(sql_file.read())
    con.commit()
    con.close()


def clean_database(db_path, clean_db_path):
    """
    Cleans and prepares a new database using the schema files

    Parameters
    ----------
    db_path : ``str``
        path to original C3S database file
    clean_db_path : ``str``
        path to clean database file
    include_path : ``str``
        path to SQLite3 schema files
    """
    # create clean database with identical structrue as C3S
    create_clean_db(clean_db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT g_id, name, adduct, mass, z, mz, ccs, smi, chem_class_label, src_tag, ccs_type, ccs_method FROM master"
    )
    data = cursor.fetchall()
    grouped = {}
    for entry in data:
        # normalize name to lowercase and round mz for grouping
        normalized_name = entry[1].lower()
        # round mz to the nearest integer for grouping
        rounded_mz = round(entry[5], 0)
        key = (
            normalized_name,
            entry[2],
            rounded_mz,
        )
        # group by name, adduct, and rounded mz
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(
            {
                "g_id": entry[0],
                "name": entry[1],
                "adduct": entry[2],
                "mass": entry[3],
                "z": entry[4],
                "mz": entry[5],
                "ccs": entry[6],
                "smi": entry[7],
                "chem_class_label": entry[8],
                "src_tag": entry[9],
                "ccs_type": entry[10],
                "ccs_method": entry[11],
            }
        )
    clean_conn = sqlite3.connect(clean_db_path)
    clean_cursor = clean_conn.cursor()
    for group, entries in grouped.items():
        # process chemical groups
        processed_ccs = process_entries(entries)
        unique_ccs = set(processed_ccs)
        # create a set of unique CCS values to avoid duplicates
        for entry, ccs_value in zip(entries, processed_ccs):
            if ccs_value in unique_ccs:
                clean_cursor.execute(
                    "INSERT INTO master VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        entry["g_id"],
                        entry["name"],
                        entry["adduct"],
                        entry["mass"],
                        entry["z"],
                        entry["mz"],
                        ccs_value,
                        entry["smi"],
                        entry["chem_class_label"],
                        entry["src_tag"],
                        entry["ccs_type"],
                        entry["ccs_method"],
                    ),
                )
                # remove this CCS values from the set after inserting
                unique_ccs.remove(ccs_value)
    clean_conn.commit()
    clean_conn.close()
    conn.close()
    print(f"Database cleaned and saved as {clean_db_path}")


if __name__ == "__main__":
    clean_database("C3S.db", "C3S_clean.db")
