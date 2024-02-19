# file with functions for mainipulated the 6 extracted datasets

import sqlite3
from fuzzywuzzy import fuzz

def create_fifa_table():
    conn = sqlite3.connect('football.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    # Joining the fifa tables into one dataset using the fifa ID attribute
    cur.execute("SELECT fifa19.name, fifa19.age, fifa19.overall, fifa19.value_mil_euro, fifa20.age, fifa20.overall, "
                "fifa20.value_mil_euro FROM fifa19 INNER JOIN fifa20 ON fifa19.id=fifa20.id")
    playerRows = cur.fetchall()

    cur.execute("DROP TABLE IF EXISTS fifa")
    # Change value to INTEGER
    cur.execute("CREATE TABLE fifa (name TEXT, age_19 INTEGER, overall_19 INTEGER, value_mil_euro_19 REAL, age_20 INTEGER, "
                "overall_20 INTEGER, value_mil_euro_20 REAL)")

    for row in playerRows:
        cur.execute("INSERT INTO fifa VALUES (?,?,?,?,?,?,?)", row)

    # take out these two?
    conn.commit()
    conn.close()

# joining the two laliga tables into one dataset showing the change in performance 2018 to 2019
def create_laliga_table():
    # take out these two?
    conn = sqlite3.connect('football.db', detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    # find out what detect_types means
    cur = conn.cursor()

    # Will need to use fullNames from each set to create unique keys associated with each players -- used to join the tables with
    cur.execute("SELECT fullName FROM laliga18")
    players18 = cur.fetchall()
    players18 = [player[0] for player in players18]

    # change to select *
    cur.execute("SELECT fullName FROM laliga19")
    players19 = cur.fetchall()
    players19 = [player[0] for player in players19]

    key = 0

    # now there are 35 attributes
    cur.execute("ALTER TABLE laliga18 ADD key INTEGER")
    cur.execute("ALTER TABLE laliga19 ADD key INTEGER")

    # Since we are matching names from the same database (that only have slight differences), we use a 100% fuzzy match

    # create and add key whenever there is a match
    for name1 in players18:
        for name2 in players19:
            if fuzz.token_set_ratio(name1, name2) == 100:
                cur.execute("UPDATE laliga18 SET key=? WHERE fullName=?", (key, name1))
                cur.execute("UPDATE laliga19 SET key=? WHERE fullName=?", (key, name2))
                key += 1
                break


    # Join using (newly created) unique key
    # the integer attributes represent change from the 2018 season to the 2019 season; there are 34 attributes
    sqlAttrib = "fullName TEXT, team TEXT, apps_tot_chg INTEGER, apps_st_chg INTEGER, mins_chg INTEGER, pos TEXT, subIn_chg " \
                "INTEGER, subOut_chg INTEGER, shots_tot_chg INTEGER, shots_on_chg INTEGER, goals_chg INTEGER, assists_chg " \
                "INTEGER, saves_chg INTEGER, conceded_chg INTEGER, passes_chg INTEGER, key_passes_chg INTEGER, pass_accur_chg " \
                "INTEGER, tackles_chg INTEGER, block_chg INTEGER, intcp_chg INTEGER, duels_chg INTEGER, duels_won_chg INTEGER," \
                " dribbles_chg INTEGER, dribbles_succ_chg INTEGER, fouls_drawn_chg INTEGER, fouls_chg INTEGER, yl_chg INTEGER," \
                " two_yl_chg INTEGER, red_chg INTEGER, pen_won_chg INTEGER, pen_commit_chg INTEGER, pen_score_chg INTEGER, " \
                "pen_miss_chg INTEGER, pen_save_chg INTEGER"

    # drop table before creating it -- if it exists
    cur.execute("DROP TABLE IF EXISTS laliga")
    # create a dataset table
    cur.execute("CREATE TABLE laliga (" + sqlAttrib + ")")


    # selecting values for change between the 2018 and 2019 integer attributes
    # is there a faster way to do this? ??? !!! ASK TA
    cur.execute("SELECT laliga19.fullName, laliga19.team, laliga19.apps_total-laliga18.apps_total, laliga19.apps_started-"
                "laliga18.apps_started, laliga19.mins-laliga18.mins, laliga19.pos, laliga19.subIn-laliga18.subIn, "
                "laliga19.subOut-laliga18.subIn, laliga19.shots_total-laliga18.shots_total, laliga19.shots_on-laliga18.shots_on,"
                " laliga19.goals-laliga18.goals, laliga19.assists-laliga18.assists, laliga19.saves-laliga18.saves, "
                "laliga19.conceded-laliga18.saves, laliga19.passes-laliga18.passes, laliga19.key_passes-laliga18.passes, "
                "laliga19.pass_accur-laliga18.pass_accur, laliga19.tackles-laliga18.tackles, laliga19.block-laliga18.block, "
                "laliga19.intercept-laliga18.intercept, laliga19.duels-laliga18.duels, laliga19.duels_won-laliga18.duels_won, "
                "laliga19.dribbles-laliga18.dribbles, laliga19.dribbles_succ-laliga18.dribbles_succ, laliga19.fouls_drawn-"
                "laliga18.fouls_drawn, laliga19.fouls-laliga18.fouls, laliga19.yellows-laliga18.yellows, laliga19.two_yellows-"
                "laliga18.two_yellows, laliga19.red-laliga18.red, laliga19.pen_won-laliga18.pen_won, laliga19.pen_commit-"
                "laliga18.pen_commit, laliga19.pen_score-laliga18.pen_score, laliga19.pen_miss-laliga18.pen_miss, "
                "laliga19.pen_save-laliga18.pen_save FROM laliga18 INNER JOIN laliga19 ON laliga18.key=laliga19.key")
    chg_stats = cur.fetchall()

    for tup in chg_stats:
        cur.execute("INSERT INTO laliga VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tup)

    # take out these two?
    conn.commit()
    conn.close()


def createDummies(tableName, db="football.db"):
    conn = sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    # find out what detect_types means
    cur = conn.cursor()

    # creating position dummies from position attribute
    cur.execute("ALTER TABLE " + tableName + " ADD attacker INTEGER")
    cur.execute("UPDATE " + tableName + " SET attacker=1 WHERE pos='Attacker'")
    cur.execute("UPDATE " + tableName + " SET attacker=0 WHERE attacker IS NULL")

    cur.execute("ALTER TABLE " + tableName + " ADD midfielder INTEGER")
    cur.execute("UPDATE " + tableName + " SET midfielder=1 WHERE pos='Midfielder'")
    cur.execute("UPDATE " + tableName + " SET midfielder=0 WHERE midfielder IS NULL")

    cur.execute("ALTER TABLE " + tableName + " ADD defender INTEGER")
    cur.execute("UPDATE " + tableName + " SET defender=1 WHERE pos='Defender'")
    cur.execute("UPDATE " + tableName + " SET defender=0 WHERE defender IS NULL")

    cur.execute("ALTER TABLE " + tableName + " ADD goalkeeper INTEGER")
    cur.execute("UPDATE " + tableName + " SET goalkeeper=1 WHERE pos='Goalkeeper'")
    cur.execute("UPDATE " + tableName + " SET goalkeeper=0 WHERE goalkeeper IS NULL")

    conn.commit()
    conn.close()


# joining the tables from laliga and fifa into one dataset
def create_allCurrentStats_table():
    # take out these two?
    conn = sqlite3.connect('football.db', detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    # find out what detect_types means
    cur = conn.cursor()

    # creating key to join them with

    cur.execute("SELECT fullName FROM laliga")
    ll_players = cur.fetchall()
    ll_playersL = [player[0] for player in ll_players]

    cur.execute("SELECT name FROM fifa")
    f_players = cur.fetchall()
    f_playersL = [player[0] for player in f_players]

    key = 0
    cur.execute("ALTER TABLE laliga ADD key INTEGER")
    cur.execute("ALTER TABLE fifa ADD key INTEGER")

    # create and add key whenever there is a match. Then join based on the match
    num = 100
    # 88% is most useful metric based on my testing
    while num >= 88:
        for name1 in ll_playersL:
            for name2 in f_playersL:
                if fuzz.token_set_ratio(name1, name2) == num:
                    cur.execute("UPDATE laliga SET key=? WHERE fullName=?", (key, name1))
                    cur.execute("UPDATE fifa SET key=? WHERE name=?", (key, name2))
                    key += 1
                    break
        num -= 1


    # the integer attributes represent change from the 2018 season to the 2019 season; there are 34 attributes
    sqlAttrib = "fullName TEXT, team TEXT, apps_tot_chg INTEGER, apps_st_chg INTEGER, mins_chg INTEGER, pos TEXT, subIn_chg " \
                "INTEGER, subOut_chg INTEGER, shots_tot_chg INTEGER, shots_on_chg INTEGER, goals_chg INTEGER, assists_chg " \
                "INTEGER, saves_chg INTEGER, conceded_chg INTEGER, passes_chg INTEGER, key_passes_chg INTEGER, pass_accur_chg " \
                "INTEGER, tackles_chg INTEGER, block_chg INTEGER, intcp_chg INTEGER, duels_chg INTEGER, duels_won_chg INTEGER," \
                " dribbles_chg INTEGER, dribbles_succ_chg INTEGER, fouls_drawn_chg INTEGER, fouls_chg INTEGER, yl_chg INTEGER," \
                " two_yl_chg INTEGER, red_chg INTEGER, pen_won_chg INTEGER, pen_commit_chg INTEGER, pen_score_chg INTEGER, " \
                "pen_miss_chg INTEGER, pen_save_chg INTEGER, key INTEGER, " \
                "name TEXT, age_19 INTEGER, overall_19 INTEGER, value_mil_euro_19 " \
                "REAL, age_20 INTEGER, overall_20 INTEGER, value_mil_euro_20 REAL, f_key INTEGER"

    cur.execute("DROP TABLE IF EXISTS allCurrentStats")
    cur.execute("CREATE TABLE allCurrentStats (" + sqlAttrib + ")")

    cur.execute("SELECT laliga.*, fifa.* FROM laliga INNER JOIN fifa ON laliga.key=fifa.key")
    allStats = cur.fetchall()

    for tup in allStats:
        cur.execute("INSERT INTO allCurrentStats VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
                    "?,?,?,?,?,?,?,?)", tup)

    # take out these two?
    conn.commit()
    conn.close()

def update_laliga_keys(db="football"):
    conn = sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    cur.execute("UPDATE laliga19 SET key=NULL")
    cur.execute("UPDATE laliga18 SET key=NULL")
    cur.execute("SELECT laliga.key, laliga19.fullName FROM laliga19 INNER JOIN laliga ON laliga19.fullName=laliga.fullName")
    keys = cur.fetchall()

    # print(len(keys))
    # for i in keys:
    #     print(i)

    for tup in keys:
        cur.execute("UPDATE laliga19 SET key=? WHERE fullName=?", tup)
        cur.execute("UPDATE laliga18 SET key=? WHERE fullName=?", tup)

    conn.commit()
    conn.close()


def create_allStatsHist_table(db='football.db'):
    update_laliga_keys(db)

    conn = sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    # need to first create a table with all the relevant attributes for "fifa", "laliga19", "careerStats"
    # drop table before creating it -- if it exists

    # createDummies("laliga19")

    # the integer attributes represent change from the historical averages to the 2019 season; there are 21 attributes
    sqlAttrib = "name TEXT, age_19 INTEGER, overall_19 INTEGER, value_mil_euro_19 REAL, age_20 INTEGER, " \
                "overall_20 INTEGER, value_mil_euro_20 REAL, key INTEGER," \
                "fullName TEXT, apps_total INTEGER, apps_started INTEGER, mins INTEGER, pos TEXT, " \
                "subIn INTEGER, subOut INTEGER, goals INTEGER, assists INTEGER, " \
                "conceded INTEGER, yellow INTEGER, two_yellow INTEGER, red INTEGER, pen_score INTEGER"

    cur.execute("DROP TABLE IF EXISTS allStatsHist")
    # create a dataset table
    cur.execute("CREATE TABLE allStatsHist (" + sqlAttrib + ")")


    # selecting values for change between the 2018 and 2019 integer attributes
    cur.execute("SELECT fifa.*, laliga19.fullName, laliga19.apps_total, laliga19.apps_started, laliga19.mins, laliga19.pos, "
                "laliga19.subIn, laliga19.subOut, laliga19.goals, laliga19.assists, laliga19.conceded, laliga19.yellows, "
                "laliga19.two_yellows, laliga19.red, laliga19.pen_score "
                "FROM laliga19 INNER JOIN fifa ON laliga19.key=fifa.key")
    stats = cur.fetchall()

    for tup in stats:
        cur.execute("INSERT INTO allStatsHist VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tup)


    # the integer attributes represent change from the historical averages to the 2019 season; there are 21 attributes
    sqlAttrib = "name TEXT, age_19 INTEGER, overall_19 INTEGER, value_mil_chg REAL, age_20 INTEGER, " \
                "overall_20 INTEGER, apps_chg REAL, mins_chg REAL, pos TEXT, " \
                "subIn_chg REAL, subOut_chg REAL, goals_chg REAL, assists_chg REAL, " \
                "conceded_chg REAL, yellow_chg REAL, two_yellow_chg REAL, red_chg REAL, pen_score_chg REAL, " \
                "key INTEGER"

    # I mulitply by 1.0 or else otherwise the value stays an integer
    # selecting values for change between the historical averages and 2019 integer attributes
    cur.execute("SELECT allStatsHist.fullName, allStatsHist.age_19, allStatsHist.overall_19, "
                "allStatsHist.value_mil_euro_20-allStatsHist.value_mil_euro_19, "
                "allStatsHist.age_20, allStatsHist.overall_20, "
                "allStatsHist.apps_total-careerStats.apps*1.0/careerStats.seasons, "
                "allStatsHist.mins-careerStats.mins*1.0/careerStats.seasons, allStatsHist.pos, "
                "allStatsHist.subIn-careerStats.subOn*1.0/careerStats.seasons, allStatsHist.subOut-careerStats.subOut*1.0/"
                "careerStats.seasons, allStatsHist.goals-careerStats.goals*1.0/careerStats.seasons, "
                "allStatsHist.assists-careerStats.assists*1.0/careerStats.seasons, "
                "allStatsHist.conceded-careerStats.conceded*1.0/careerStats.seasons, allStatsHist.yellow-"
                "careerStats.yellows*1.0/careerStats.seasons, allStatsHist.two_yellow-careerStats.two_yellows*1.0/"
                "careerStats.seasons, allStatsHist.red-careerStats.red*1.0/careerStats.seasons, allStatsHist.pen_score-"
                "careerStats.pen_goals*1.0/careerStats.seasons, allStatsHist.key "
                "FROM allStatsHist LEFT JOIN careerStats ON allStatsHist.key=careerStats.key")
    stats = cur.fetchall()

    cur.execute("DROP TABLE IF EXISTS allStatsHist")
    # create a dataset table
    cur.execute("CREATE TABLE allStatsHist (" + sqlAttrib + ")")

    for tup in stats:
        cur.execute("INSERT INTO allStatsHist VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tup)

    # take out these two?
    conn.commit()
    conn.close()


