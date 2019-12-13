## Installation

1. **Download** the entire **repository**.
2. Make sure you have **Python 3** and **pip** installed.
3. **Install** the **requirements** from requirements.txt with `pip install -r requirements.txt`
4. See "Mine Data" section for further information.



## Mine Data

The entry point for mining and extracting data from transfer market is: main.py in the project root folder. The leagues to select the clubs from are specified in "/data/leagues_filter.json".



**Extract all transfers from the given clubs for the given period:**
`main.py extract-transfers-club --file "/data/clubs.json" --year-start "2000" --year-end "2020"`

**Extract all clubs from the specified leagues for the given period:**
`main.py extract-leagues-club --file "/data/clubs.json" --year-start "2000" --year-end "2020"`