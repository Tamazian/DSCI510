# API-FOOTBALL dataset: 2 tables

import requests
import time
import sqlite3
import sys
from tqdm import tqdm


# function to resend request if the host server doesn't send a response for 5 seconds after request
def request_retry(query):
    url = "https://api-football-v1.p.rapidapi.com/v3/players"
    key = "1919796a7fmshd8d34f2ce4684ecp1de551jsnf5e797a9bb31"
    host = "api-football-v1.p.rapidapi.com"

    # mandatory header parameters
    headers = {"x-rapidapi-key": key, "x-rapidapi-host": host}
    # optional parameters that specify my query
        # 140 is the number API-FOOTBALL assigned for the Spanish League

    cond = 0
    while cond < 5:
        try:
            response = requests.get(url, headers=headers, params=query, timeout=5)
            cond = 5
            return response
        except:
            time.sleep(5)
            cond += 1


def extract_dataset2():
    # create SQL database
    conn = sqlite3.connect('football.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    # 34 attributes
    sqlAttrib = "fullName TEXT, name TEXT, team TEXT, apps_total INTEGER, apps_started INTEGER, mins INTEGER, pos TEXT, " \
                "subIn INTEGER, subOut INTEGER, shots_total INTEGER, shots_on INTEGER, goals INTEGER, assists INTEGER, " \
                "saves INTEGER, conceded INTEGER, passes INTEGER, key_passes INTEGER, pass_accur INTEGER, tackles INTEGER" \
                ", block INTEGER, intercept INTEGER, duels INTEGER, duels_won INTEGER, dribbles INTEGER, dribbles_succ " \
                "INTEGER, fouls_drawn INTEGER, fouls INTEGER, yellows INTEGER, two_yellows INTEGER, red INTEGER, pen_won " \
                "INTEGER, pen_commit INTEGER, pen_score INTEGER, pen_miss INTEGER, pen_save INTEGER"

    # drop table before creating it -- if it exists
    cur.execute("DROP TABLE IF EXISTS laliga18")
    # create a dataset table
    cur.execute("CREATE TABLE laliga18 (" + sqlAttrib + ")")

    cur.execute("DROP TABLE IF EXISTS laliga19")
    cur.execute("CREATE TABLE laliga19 (" + sqlAttrib + ")")


    # url = "https://api-football-v1.p.rapidapi.com/v3/players"
    # key = "1919796a7fmshd8d34f2ce4684ecp1de551jsnf5e797a9bb31"
    # host = "api-football-v1.p.rapidapi.com"

    # mandatory header parameters
    # headers = {"x-rapidapi-key": key, "x-rapidapi-host": host}

    # We want data from two football seasons: 2018-19 and 2019-20
    seasons = ["2018", "2019"]

    for season in seasons:
        page = 1
        # optional parameters that specify my query; 140 is the number API-FOOTBALL assigned for the Spanish League
        query = {"league": "140", "season": season, "page": page}

        response = request_retry(query)
        contentD = response.json()

        total_pages = contentD["paging"]["total"]

        count = seasons.index(season)+1
        for page in tqdm(range(1, total_pages+1), desc="Dataset2 (table%i)" % count):
            # optional parameters that specify my query; 140 is the number API-FOOTBALL assigned to the Spanish League
            query = {"league": "140", "season": season, "page": page}

            # a small condition to not send a redundant query
            # why does this cause an error that at least one page from 2019 repeats instead of the right new page
            # if page != 1:
            response = request_retry(query)
            contentD = response.json()

            # just an arbitrary number greater than 1 (that starts the loop)
            numPlayers = 100
            num = 0
            while num < numPlayers:
                # number of players in the response
                numPlayers = contentD["results"]

                # It is a list of dictionaries including all player information
                playerInfoL = contentD["response"]

                fullName = playerInfoL[num]["player"]["firstname"] + " " + playerInfoL[num]["player"]["lastname"]
                name = playerInfoL[num]["player"]["name"]

                # It is a dictionaries with the relevant performance statistics
                statisticsD = playerInfoL[num]["statistics"][0]

                # Team name is the value of key in a dictionary nested in the statistics dictionary
                team = statisticsD["team"]["name"]
                # total game appearances
                apps_tot = statisticsD["games"]["appearences"]
                # game appearance started
                apps_started = statisticsD["games"]["lineups"]
                # total minutes played
                mins = statisticsD["games"]["minutes"]
                # position
                pos = statisticsD["games"]["position"]
                # games subbed in
                subIn = statisticsD["substitutes"]["in"]
                # games subbed out
                subOut = statisticsD["substitutes"]["out"]
                # total shots
                shots_tot = statisticsD["shots"]["total"]
                # shots on target
                shots_on = statisticsD["shots"]["on"]
                # goals
                goals = statisticsD["goals"]["total"]
                # assists
                assists = statisticsD["goals"]["assists"]
                # think of how to include goalkeepers in the analysis
                # goals saved
                saves = statisticsD["goals"]["saves"]
                # goals conceded
                conc = statisticsD["goals"]["conceded"]
                # total passes
                passes = statisticsD["passes"]["total"]
                # passes that led to goal
                keyPasses = statisticsD["passes"]["key"]
                # pass accuracy (percentage)
                accur = statisticsD["passes"]["accuracy"]
                # tackles
                tackles = statisticsD["tackles"]["total"]
                # blocks
                blocks = statisticsD["tackles"]["blocks"]
                # interceptions
                interc = statisticsD["tackles"]["interceptions"]
                # total duels (for ball)
                duels = statisticsD["duels"]["total"]
                # duels won
                duelsWon = statisticsD["duels"]["won"]
                # dribbles attempted
                dribbles = statisticsD["dribbles"]["attempts"]
                # dribbles succeeded
                drib_succ = statisticsD["dribbles"]["success"]
                # fouls drawn
                fouls_drawn = statisticsD["fouls"]["drawn"]
                # fouls committed
                fouls = statisticsD["fouls"]["committed"]
                # yellow card
                yel = statisticsD["cards"]["yellow"]
                # double yellow card
                yel2 = statisticsD["cards"]["yellowred"]
                # red card
                red = statisticsD["cards"]["red"]
                # penalty won
                pen_won = statisticsD["penalty"]["won"]
                # penalty committed (in favor of the other team)
                pen_com = statisticsD["penalty"]["commited"]
                # penalty scored
                pen_score = statisticsD["penalty"]["scored"]
                # penalty missed
                pen_miss = statisticsD["penalty"]["missed"]
                # penalty saved
                pen_save = statisticsD["penalty"]["saved"]

                stats = [fullName, name, team, apps_tot, apps_started, mins, pos, subIn, subOut, shots_tot, shots_on, goals,
                         assists, saves, conc, passes, keyPasses, accur, tackles, blocks, interc, duels, duelsWon, dribbles,
                         drib_succ, fouls_drawn, fouls, yel, yel2, red, pen_won, pen_com, pen_score, pen_miss, pen_save]

                for stat in stats:
                    if stat is None:
                        stats[stats.index(stat)] = 0

                if season == "2018":
                    cur.execute("INSERT INTO laliga18 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
                                "?,?,?,?)", tuple(stats))
                elif season == "2019":
                    cur.execute("INSERT INTO laliga19 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
                                "?,?,?,?)", tuple(stats))
                num += 1
            # # Extraction progress Bar
            # if page != 1000:
            #     print("Page", page, "out of", total_pages, "completed for season", season)
            page += 1
            # ASK TA
            # Got a random KeyError for "paging" which usually works perfectly. I haven't tested too often, but sleep helped
            time.sleep(3)

    conn.commit()
    conn.close()


