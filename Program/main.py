import sqlite3
import pandas as pd
import sys
from dataset1 import scrape_dataset1
from dataset2 import extract_dataset2
from dataset3 import scrape_dataset3
from regression import *




def dataset_sample_print():
    # conn = sqlite3.connect(sys.argv[2], detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    # cur = conn.cursor()

    # with out this, the datasets with many attributes get truncated at 5 columns
    pd.options.display.max_columns = None
    pd.options.display.width = 220

    print("\nThe data for my analysis is extracted from 'SoFIFA', 'API-Football', and 'Transfermarkt'")
    print("Below are the sample rows for the 5 original datasets extracted from the 3 three sources, before any other "
          "data manipulation")

    print("\nDataset 1 (table 1): 'fifa19'")
    cur.execute("SELECT * FROM fifa19")
    rowNums = len(cur.fetchall())
    cur.execute("SELECT * FROM fifa19")
    colNums = len(cur.fetchone())
    print("Number of total rows:", rowNums, "\nNumber of total columns:", colNums, "\n____________________________")
    rows = pd.read_sql_query("SELECT * FROM fifa19 LIMIT 5", conn)
    print(rows)

    print("\nDataset 1 (table 2): 'fifa20'")
    cur.execute("SELECT * FROM fifa20")
    rowNums = len(cur.fetchall())
    cur.execute("SELECT * FROM fifa20")
    colNums = len(cur.fetchone())
    print("Number of total rows:", rowNums, "\nNumber of total columns:", colNums, "\n____________________________")
    rows = pd.read_sql_query("SELECT * FROM fifa20 LIMIT 5", conn)
    print(rows)

    print("\nDataset 2 (table 1): 'laliga18'")
    cur.execute("SELECT * FROM laliga18")
    rowNums = len(cur.fetchall())
    cur.execute("SELECT * FROM laliga18")
    colNums = len(cur.fetchone())
    print("Number of total rows:", rowNums, "\nNumber of total columns:", colNums, "\n____________________________")
    rows = pd.read_sql_query("SELECT * FROM laliga18 LIMIT 5", conn)
    print(rows)

    print("\nDataset 2 (table 2): 'laliga19'")
    cur.execute("SELECT * FROM laliga19")
    rowNums = len(cur.fetchall())
    cur.execute("SELECT * FROM laliga19")
    colNums = len(cur.fetchone())
    print("Number of total rows:", rowNums, "\nNumber of total columns:", colNums, "\n____________________________")
    rows = pd.read_sql_query("SELECT * FROM laliga19 LIMIT 5", conn)
    print(rows)

    print("\nDataset 3 (table 1): 'careerStats'")
    cur.execute("SELECT * FROM careerStats")
    rowNums = len(cur.fetchall())
    cur.execute("SELECT * FROM careerStats")
    colNums = len(cur.fetchone())
    print("Number of total rows:", rowNums, "\nNumber of total columns:", colNums, "\n____________________________")
    rows = pd.read_sql_query("SELECT * FROM careerStats LIMIT 5", conn)
    print(rows)

    print("\nMy analysis about footballer player value is in large part based on the CHANGE in their performance.")
    print("So, the accuracy and credibility of my final regression analysis is directly dependent on what approach I use"
          "to calculate the change.")
    print("There where 3 different methodologies I was considering:"
          "\n\tcalculate change by subtracting the players' 2018 season performance stats from its 2019 counterpart,"
          "\n\tcalculate change by subtracting the players' historical, performance average from the 2019 season stats, or"
          "\n\tcalculate change by subtracting the players' historical, performance average -- calculated with a greater "
          "weight placed on the most recent (2018) season results -- from the 2019 season stats.")
    print("The related dataset (ie table) for the 3 methods are "
          "'allCurrentStats', 'allStatsHist_Unweighted', and 'allStatsHist'")
    print("Below are 3 regression table outputs for the 3 methods -- respectively.")
    print("The chosen variables here are attributes I thought would be relevant for this test, but do not (necessarily) "
          "represent the final regression model's configuration.")
    print("The 2019 season attributes are also all same for each regression, in order to to maintain comparability.")

    # conn.commit()
    # conn.close()


if (len(sys.argv) == 3) and (sys.argv[1] == "--static") and ("main.py" in sys.argv[0]):
    conn = sqlite3.connect(sys.argv[2], detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    pd.options.display.max_columns = None
    pd.options.display.width = 220

    dataset_sample_print()

    conn.commit()
    conn.close()

    print("\nReg1")
    regTest1(sys.argv[2])
    print("\nReg2")
    regTest2(sys.argv[2])
    print("\nReg3")
    regTest3(sys.argv[2])

    print("\n\nFinal Regression Output")
    fin_reg(sys.argv[2])

elif (len (sys.argv) == 1) and ("main.py" in sys.argv[0]):
    scrape_dataset1()
    extract_dataset2()
    scrape_dataset3()

    conn = sqlite3.connect("football.db", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    pd.options.display.max_columns = None
    pd.options.display.width = 220

    dataset_sample_print()

    conn.commit()
    conn.close()

    print("\nRegression Table: Method 1")
    regTest1()
    print("\nRegression Table: Method 2")
    regTest2()
    print("\nRegression Table: Method 3")
    regTest3()

    fin_reg()

else:
    print("Oops, it looks like you entered an improper input. Please try again.")


# conn.commit()
# conn.close()

