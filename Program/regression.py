import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
from sqlTable import *



pd.options.display.max_columns = None
pd.options.display.width = 220


# before doing a final regression, I need to determine of my 3 methodologies yields the best results

# first is a regression using change attributes: 2019 stats - 2018 stats
# this is just a test regression, so we just need a few relevant attributes that are consistent for all three  methods
def regTest1(db='football.db'):
    conn = sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

    createDummies("allCurrentStats", db)

    df = pd.read_sql_query("SELECT * FROM allCurrentStats", conn)
    # print(df.head())
    # print()

    # y represents the dependent variable in a regression
    y = df["value_mil_euro_20"]-df["value_mil_euro_19"]
    # print(y)

    # xk represents the dependent variables
    xk = df[["age_20", "mins_chg", "conceded_chg", "goals_chg", "red_chg", "assists_chg", "goalkeeper", "midfielder", "attacker"]]

    # will use x to represent the constant
    x = sm.add_constant(xk)

    # plt.scatter(x1, y)
    # plt.xlabel("overall", fontsize=20)
    # plt.ylabel("value", fontsize=20)
    # print(plt.show())

    results = sm.OLS(y, x).fit()
    print(results.summary())

    conn.commit()
    conn.close()


# second is a regression using change attributes: 2019 stats - historical average stats
def regTest2(db='football.db'):
    create_allStatsHist_table(db)

    conn = sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

    createDummies("allStatsHist", db)

    df = pd.read_sql_query("SELECT * FROM allStatsHist", conn)

    # y represents the dependent variable in a regression
    y = df["value_mil_chg"]
    # print(y)

    # xk represents the dependent variables
    xk = df[["age_20", "mins_chg", "conceded_chg", "goals_chg", "red_chg", "assists_chg", "goalkeeper", "midfielder", "attacker"]]

    # will use x to represent the constant
    x = sm.add_constant(xk)

    results = sm.OLS(y, x).fit()
    print(results.summary())

    conn.commit()
    conn.close()


# third is a regression using change attributes: 2019 stats - historical average stats
def regTest3(db='football.db'):
    # duplicating the careerStats and allStatsHist table, since I don't want the original to be lost
    conn = sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    cur.execute("CREATE TABLE allStatsHist_Unweighted AS SELECT * FROM allStatsHist")
    cur.execute("CREATE TABLE careerStats_Unweighted AS SELECT * FROM careerStats")

    conn.commit()
    conn.close()


    conn = sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    # adjusting the historical values to have a greater weight for the most recent season
    cur.execute("SELECT careerStats.name, careerStats.seasons+3, careerStats.apps+laliga18.apps_total*3, careerStats.goals+"
                "laliga18.goals*3, "
                "careerStats.assists+laliga18.assists*3, careerStats.own_goals, careerStats.subOn+laliga18.subIn*3, "
                "careerStats.subOut+laliga18.subOut*3, "
                "careerStats.yellows+laliga18.yellows*3, careerStats.two_yellows+laliga18.two_yellows*3, careerStats.red+"
                "laliga18.red*3, careerStats.conceded+laliga18.conceded*"
                "3, careerStats.clean_sheets, careerStats.pen_goals+laliga18.pen_score*3, careerStats.goals_per_min, "
                "careerStats.mins, "
                "careerStats.key FROM careerStats INNER JOIN laliga18 ON laliga18.key=careerStats.key")
    stats = cur.fetchall()

    cur.execute("DROP TABLE IF EXISTS careerStats")
    # adding a W to the attribute names to indicate it is weighted
    cur.execute("CREATE TABLE careerStats (name TEXT, seasons INTEGER, apps INTEGER, goals INTEGER, assists INTEGER, "
                "own_goals INTEGER, subOn INTEGER, subOut INTEGER, yellows INTEGER, two_yellows INTEGER, red INTEGER, "
                "conceded INTEGER, clean_sheets INTEGER, pen_goals INTEGER, goals_per_min INTEGER, mins INTEGER, key "
                "INTEGER)")

    for tup in stats:
        cur.execute("INSERT INTO careerStats VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tup)

    conn.commit()
    conn.close()

    # allStatsHist now represents the weighted results, the unweighted is allStatsHist_Unweighted
    regTest2(db)


def fin_reg(db='football.db'):
    print("\nAn effective and simple way to determine which of the 3 methods deliver the best results is to look at the R-"
          "squared, \nwhich measures how much of the variability in 'change in value' is explained by the chosen variables.")
    print("\nAlthough, unfortunately, none of the values are as high I would have hoped, the 0.335 R-squared measured from "
          "the third method is clearly the highest.")
    print("So, our (subsequent) final regression analysis will be using attributes where the change is calculated by \n"
          "subtracting the 2019 performance stats by historical averages weighted heavily towards the 2018 season results.")
    print("This means the regression will be run on the 'allStatsHist' dataset -- a sample of which can be seen below.")

    conn = sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    print("\nFinal Dataset: 'allStatsHist'")
    cur.execute("SELECT * FROM allStatsHist")
    rowNums = len(cur.fetchall())
    cur.execute("SELECT * FROM allStatsHist")
    colNums = len(cur.fetchone())
    print("Number of total rows:", rowNums, "\nNumber of total columns:", colNums, "\n____________________________")
    rows = pd.read_sql_query("SELECT * FROM allStatsHist LIMIT 5", conn)
    print(rows)

    # createDummies("allStatsHist")

    df = pd.read_sql_query("SELECT * FROM allStatsHist", conn)

    # y represents the dependent variable in a regression
    y = df["value_mil_chg"]
    # print(y)

    # xk represents the dependent variables
    xk = df[["age_20", "overall_19", "mins_chg", "apps_chg", "goals_chg", "conceded_chg", "yellow_chg", "assists_chg",
            "goalkeeper", "defender", "midfielder"]]

    # will use x to represent the constant
    x = sm.add_constant(xk)

    results = sm.OLS(y, x).fit()
    print("\n\nFinal Regression Output")
    print(results.summary())

    x1 = df["assists_chg"]
    plt.scatter(x1, y)
    plt.title("Positive Relationship Sample", fontsize=25)
    plt.xlabel("Change in Assists", fontsize=20)
    plt.ylabel("Change in Value", fontsize=20)
    plt.show()

    x2 = df["age_20"]
    plt.scatter(x2, y)
    plt.title("Negative Relationship Sample", fontsize=25)
    plt.xlabel("Age", fontsize=20)
    plt.ylabel("Change in Value", fontsize=20)
    plt.show()

    conn.commit()
    conn.close()

    print("\n\nThese two scatterplots (which popped-up) are merely visually aids to interpret the relationships between a "
          "couple key attributes and the dependent variable ('change in value').")
    print("The first scatterplot is for 'change in assists', which has the strongest positive impact on 'change in value")
    print("The second scatterplot is for 'age', which displays a significiant, negative relationship with 'change in value")

    print("\nFor futher detail on the outputs and analysis, please refer to the Project Description pdf.")


