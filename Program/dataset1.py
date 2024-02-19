# SoFIFA dataset

from bs4 import BeautifulSoup
import requests
import sqlite3
import time
from tqdm import tqdm


# function to resend request if the host server doesn't send a response for 5 seconds after request
def request_retry(url):
    headers = {'User-Agent':
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    cond = 0
    while cond < 5:
        try:
            response = requests.get(url, headers=headers, timeout=5)
            cond = 5
            return response
        except:
            time.sleep(5)
            cond += 1


def scrape_dataset1():
    # create SQL database
    conn = sqlite3.connect('football.db', detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    # drop table before creating it -- if it exists
    cur.execute("DROP TABLE IF EXISTS fifa20")
    # create a dataset table
    cur.execute("CREATE TABLE fifa20 (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, overall INTEGER, value_mil_euro REAL)")

    cur.execute("DROP TABLE IF EXISTS fifa19")
    cur.execute("CREATE TABLE fifa19 (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, overall INTEGER, value_mil_euro REAL)")

    # starts with a webpage of all la liga teams that were part of la liga in the 2019-20 season
    url = "https://sofifa.com/teams?lg=53&r=200001&set=true"
    headers = {'User-Agent':
               'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

    response = request_retry(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    domain = "https://sofifa.com"

    paths = []
    paths_19 = []
    paths_20 = []

    # not actually a list but a <class 'bs4.element.ResultSet'>; but also in [] and can be iterated
    aTagsL = soup.find_all("a")

    # gets a list of all the url paths for the 20 La Liga teams
    for tag in aTagsL:
        if ("href" in str(tag)) and ("/team/" in tag.get("href")):
            teamPath = tag.get("href")
            paths.append(teamPath)

    # gives the content nested in the dropdown class containing the path info for all the FIFA versions
            # need attribute "class" to contain EXACTLY "dropdown". Need to use anonymous function lambda for this
    divTags = soup.find(lambda dTag: dTag.name == "div" and dTag.get("class") == ["dropdown"])

    aTagL_vers = divTags.find_all("a", attrs={"class": "bp3-menu-item"})

    for tag in aTagL_vers:
        if "FIFA 20" in tag:
            verPath = tag.get("href")

            team_url = domain + verPath
            response = request_retry(team_url)
            soup_f20 = BeautifulSoup(response.content, 'html.parser')

        if "FIFA 19" in tag:
            verPath = tag.get("href")

            team_url = domain + verPath
            response = request_retry(team_url)
            soup_f19 = BeautifulSoup(response.content, 'html.parser')
            break


    divTagsL = soup_f20.find_all(lambda dTag: dTag.name == "div" and dTag.get("class") == ["dropdown"])
    aTagL_dates = divTagsL[1].find_all("a", attrs={"class": "bp3-menu-item"})

    for tag in aTagL_dates:
        # Jul 23, 2020 : date immediately after the 2019 season
        if "Jul 23, 2020" in tag:
            interPath = tag.get("href")

            queryStart = interPath.find("?")
            query = interPath[queryStart:]

            for path in paths:
                finPath20 = path + query
                paths_20.append(finPath20)

    # MAKE THIS MORE EFFECIENT WITH A LOOP
    divTagsL = soup_f19.find_all(lambda dTag: dTag.name == "div" and dTag.get("class") == ["dropdown"])
    aTagL_dates = divTagsL[1].find_all("a", attrs={"class": "bp3-menu-item"})

    for tag in aTagL_dates:
        # Aug 15, 2019 : date immediate before the start
        if "Aug 15, 2019" in tag:
            interPath = tag.get("href")

            queryStart = interPath.find("?")
            query = interPath[queryStart:]

            for path in paths:
                finPath19 = path + query
                paths_19.append(finPath19)
            break


    finPathsT = paths_20, paths_19
    count = 1
    for pathsL in finPathsT:
        # tqdm module create a progress bar for the loop
        for path in tqdm(pathsL, desc="Dataset1 (table%i)" % count):
            # now we are at the page we want to web scrape
            url = domain + path

            # Waiting 3 seconds before sending new request as to not overwhelm the host server (and get banned)
            time.sleep(3)
            cond = 0
            while cond < 5:
                try:
                    response = requests.get(url, headers=headers, timeout=5)
                    cond = 5
                except:
                    time.sleep(5)
                    cond += 1
            soup = BeautifulSoup(response.content, 'html.parser')

            table = soup.find("table", class_="table table-hover persist-area")

            allRows = table.find_all("tr", class_=["starting", "sub", "res"])
            for row in allRows:
                # every footballer has his own unique FIFA ID (which will be used to merge the datasets)
                fifaID = row.find("img").get("id")

                name_cell = row.find_all("td", class_="col-name")
                name_cell = name_cell[0].find("a")
                # write notes
                name = str(name_cell.get("data-tooltip"))

                values = row.find_all("td", class_="col")
                for value in values:
                    if value.get("data-col") == "ae":
                        # .string attribute of a tag, gives the text inside it
                        age = int(value.string)
                    elif value.get("data-col") == "oa":
                        overall = int(value.string)
                    elif value.get("data-col") == "vl":
                        # need value as a float and not a string
                        if str(value.string)[-1] == "M":
                            value_euro = float(str(value.string)[1:-1])
                        elif str(value.string)[-1] == "K":
                            value_euro = float("0." + str(value.string)[1:-1])

                if count == 1:
                    cur.execute("INSERT INTO fifa20 VALUES (?,?,?,?,?)", (fifaID, name, age, overall, value_euro))
                else:
                    cur.execute("INSERT INTO fifa19 VALUES (?,?,?,?,?)", (fifaID, name, age, overall, value_euro))
        count += 1

    conn.commit()
    conn.close()
