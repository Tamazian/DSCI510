# transfermarkt dataset: 2 tables

from bs4 import BeautifulSoup
import requests
import sqlite3
import time
import sys
from tqdm import tqdm
from sqlTable import *
from dataset1 import request_retry


def api_request_retry(query):
    url = "https://customsearch.googleapis.com/customsearch/v1"
    key = "AIzaSyBn6qw8GjP0mydZtCqUhF4dVto--LXHmQ4"
    cx = "5164558f93c5ae5dc"

    queryString = url + "?key=" + key + "&num=5" + "&cx=" + cx + "&exactTerms=spieler" + "&q=" + query + \
                  "+Player+profile+Transfermarkt"

    cond = 0
    while cond < 5:
        try:
            response = requests.get(queryString, timeout=3)
            cond = 5
            return response
        except:
            time.sleep(3)
            cond += 1


def scrape_dataset3():
    # join the 4 already extracted datasets into 1 table, to use for extracting from the 3rd source
    create_fifa_table()
    create_laliga_table()
    create_allCurrentStats_table()


    conn = sqlite3.connect('football.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    # find out what detect_types means
    cur = conn.cursor()

    # drop table before creating it -- if it exists
    cur.execute("DROP TABLE IF EXISTS careerStats")
    # create a dataset table
    cur.execute("CREATE TABLE careerStats (name TEXT, seasons INTEGER, apps INTEGER, goals INTEGER, assists INTEGER, "
                "own_goals INTEGER, subOn INTEGER, subOut INTEGER, yellows INTEGER, two_yellows INTEGER, red INTEGER, "
                "conceded INTEGER, clean_sheets INTEGER, pen_goals INTEGER, goals_per_min INTEGER, mins INTEGER, key "
                "INTEGER)")

    # url = "https://www.transfermarkt.us/laliga/startseite/wettbewerb/ES1/plus/1?saison_id=2019"
    headers = {'User-Agent':
               'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    # domain = "https://www.transfermarkt.us"


    # web scraping players based on the specific players that made up the allCurrentStats table
    # by searching for the players in transfermarkt


    # delete?
    def player_finder(search_url):
        response = request_retry(search_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        time.sleep(0.5)
        return soup


    def player_scraper(p_name, p_id):
        # This is the link to the career stats page, we want to scrape
        fin_url = "https://www.transfermarkt.us/" + p_name + "/leistungsdaten/spieler/" + p_id + "/saison/ges/plus/1"

        # content = requests.get(url, headers=headers)
        response = request_retry(fin_url)
        fin_soup = BeautifulSoup(response.content, 'html.parser')
        time.sleep(0.5)
        return fin_soup

    cur.execute("SELECT fullName, f_key, team FROM allCurrentStats")
    playerInfo = cur.fetchall()
    # name = playerNames[0]
    # print(playerNames)
    # print(len(playerNames))

    # in a query, instead of spaces between names it is a +
    playerNamesL = ["+".join(player[0].split()) for player in playerInfo]
    teamNamesL = ["+".join(player[2].split()) for player in playerInfo]

    count = 0
    for playerName in tqdm(playerNamesL, desc="Dataset3 (table1)"):
    # for playerName in playerNamesL:
        # try except (since google search is dynamic)
        # put response in "try", "except" continue !!!

        queryString = playerName + "+" + teamNamesL[count]
        # print(queryString)
        response = api_request_retry(queryString)
        contentD = response.json()

        playerLink = contentD["items"][0]["link"]

        playerName = playerLink[playerLink.find("/", 10) + 1: playerLink.find("/", (playerLink.find("/", 10) + 2))]
        playerID = playerLink[playerLink.find("spieler") + 8:]
        # print(playerName, playerID)

        soup = player_scraper(playerName, playerID)

        statsL = []

        table = soup.find("table", class_="auflistung")
        # the len of this is the number of years since the player started his career (+ the "Overall balance" row)

        # since google search results are dynamic, there is more uncertainty, and a greater change of an unexpected error
        try:
            seasons = len(table.find_all("option"))-1
        except:
            continue

        table = soup.find("table", class_="items")

        colNamesL = []
        if not colNamesL:
            columnInfo = table.find("thead")
            columnsL = columnInfo.find_all("span", class_="icons_sprite")

            for col in columnsL:
                colName = col.get("title")
                colNamesL.append(colName)

        totalStats = table.find("tfoot")
        columnsL = totalStats.find_all("td")

        for i in range(2, len(columnsL)):
            statsL.append(columnsL[i].string)

        for stat in statsL:
            if stat == "-":
                statsL[statsL.index(stat)] = None
            elif "'" in stat:
                newStat = stat
                statL = list(newStat)
                if "." in stat:
                    statL.remove(".")
                    newStat = "".join(statL)
                statsL[statsL.index(stat)] = int(newStat[:-1])
            else:
                statsL[statsL.index(stat)] = int(stat)

        statsL.insert(0, seasons)

        nameL = playerName.split("-")
        name = (" ".join(nameL)).title()
        statsL.insert(0, name)

        # using the index to get the corresponding key for each player
        key = playerInfo[count][1]
        statsL.append(key)

        # print(statsL)
        for stat in statsL:
            if stat is None:
                statsL[statsL.index(stat)] = 0
        # print(statsL)

        if "Minutes per goal" in colNamesL:
            conceded = 0
            clean_sheets = 0
            statsL.insert(11, conceded)
            statsL.insert(12, clean_sheets)

        else:
            assists = 0
            pen_goals = 0
            goals_per_min = 0
            statsL.insert(4, assists)
            statsL.insert(13, pen_goals)
            statsL.insert(14, goals_per_min)

        # I am experiencing a weird glitch where my number of bindings is interpreted incorrectly
        # I says I supplied 18 when there are actually 17. If i remove one, it says I only suplied 16; this is a workaround
        # team = teamNamesL[count].split("+")
        # team = " ".join(team)
        # statsL.insert(17, team)



        # 18 attributes
        cur.execute("INSERT INTO careerStats VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tuple(statsL))
        count += 1

    conn.commit()
    conn.close()