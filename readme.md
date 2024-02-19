As per the instructions, my program can be executed through the terminal in two ways.

The first is the dynamic method, where everything is extracted from the web. 
The terminal argument for this execution method is simply to provide: <path_to_main.py> 

The second approach is the static methods, using the provided database "football.db" in the folder "Database".
he terminal argument for this execution method is input arguments: <path_to_main.py> --static <path_to_football.db>

There is also a else condition that simply prints an error message if neither of the two conditions are satisfied.

The web-scraping version usually take about 25 minutes two execute program and extract the data.
This number is strongly dependent on the internet conditions of who ever is using the program and the server hosts.
Unfortunately, there was no way to shorten this because both the SoFIFA and Transfermarkt hosts are prone to denying access to their servers if there are too many consecutive queries.
That is why I incorporated some sleep code after the queries. 
Without which, I have otherwise, found my computer's IP temporarily banned by the host.
I used loops with a try/except block to resend a query up to five times to greatly reduce the chance of a wrong or stalled response.
However, regardless of this fail-safe, there is no guarantee;
So I ask you to please be patient, and for the small change that an error occur please check the domains of the sources.

While testing and writing the code for the web-scraping of SoFIFA and Transfermarkt, I experienced the entire site going blank or "under construction" on multiple occasions.
This usually lasted about half a day to a day. So, if an error appears when running the code due to "None" being returned, please check the host source and try again.
Similarly sometimes their servers are overwhelmed at certain times of day and query requests don't get through, with the response return a NULL or stalling. 
After ensuring the sources are functional -- at that moment -- please try again.

On May 10th, this program is fully operational for me. 
For the very small chance the program fails due some relevant HTML code being changed during this time (in an update), please contact me so we figured something out.

If you do find yourself in a situation where you need to rerun the program (for whatever reason): 
Rerun it only AFTER deleting the previously "created" (but unfinished) football.db from whatever directory the program is executed in. 
This is different from the static database provided in the "Database" folder. Do not accidentally delete that.

The reason why the program requires manually deleting the db file is because of the limitations of sqlite3.
For any SQL table, I can precede the CREATE TABLE command with a DROP TABLE command to easily prevent such a situation. 
The problem is that SQLite does not support many SQL commands, including the one to drop individual columns.
So, when the static database tables are simply altered or updated with new columns, if the program is interrupted it will require manual action.
Otherwise there will be an error because the program will try to add a column that already exists.
The is no way to revert those smaller table changes -- while preserving the original static datasets -- within the program and with sqlite3.
For this reason I also suggest creating a duplicate of the db file for posterity and deleted any incomplete attempt. Otherwise, the zip file will need to be reopened. 



The python files are located in the the "Program" folder in the tamazian-alain-DSCI510-project folder of the zip file.
"dataset1.py" contains code for extracting the SoFIFA data. It extracts two tables from it: "fifa19" and "fifa20".
	It collects player information (i.e. value and overall skill) after the 2018-19 and 2019-20 La Liga seasons, respectively.
	The attributes in both tables — in the order they appear — are a player’s FIFA ID, their full name, their overall skill level based on extensive analysis from FIFA, and their value in euros. 
"dataset2.py" contains code for extracting the API-FOOTBALL data. It extracts two tables from it: "laliga18" and "laliga19".
	It collects performance statistics of all La Liga players in the 2018-19 and 2019-20 seasons respectively.
"dataset3.py" contains code for extracting the Transfermarkt data. This also includes SQL code to combined the 4 prior extracted tables into a single dataset.
	The "fifa19" and "fifa20" tables are joined to create "fifa". The "laliga18" and "laliga19" tables are joined to create "laliga". 
	These two are subsequently joined to create the "allCurrentStats" dataset.
	The (player) observations from "allCurrentStats" are used to specify what players to scrape data from, on Transfermarkt. 
	The dataset from the third source is a single table called "careerStats" that has historical performance values on al 320 players from  "allCurrentStats".
	The provided static db file includes the (8) datasets up to this point.

The other remaining py files used to execute the program are sqlTable.py, regression.py, and main.py.
As the names suggest, the functions in the sqlTable are for the various table "join"s and other manipulations needed. 
	Its functions are called from multiple other files like the "dataset" python files and the regression file.
The regression.py file includes the code for my regression analyses split into multiple functions. 
	These functions are called from the main.py file
The "main.py" file imports all of these files and calls the relevant functions -- in the right order -- to scrape, preprocess, and analyze the data.
	It is executed through the command line based on running the two (prior specified) arguement options. 
	It prints out samples of the original 5 extracted dataset, the final dataset used for the main regression analysis (ie "allStatsHist"), 4 regression tables, and two scatterplot GUI pop-ups.
	For the static option, it updates the "football.db" database. Executing main.py with the dynamic option creates the database in the (current) directory from scratch.

