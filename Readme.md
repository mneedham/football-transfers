## Installation

1. **Download** the entire **repository**.
2. Make sure you have **Python 3** and **pip** installed.
3. **Install** the **requirements** from requirements.txt with `pip install -r requirements.txt`
4. See "Mine Data" section for further information.



## Mine Data

The entry point for mining and extracting data from transfer market is: handler.py in the project root folder.



**Find all pages for a certain time period:**
`handler.py find-all-pages --file "C:\Users\Jan\Desktop\football-transfers\tmp\all_pages.csv" --date-start 2018-01-01 --date-end 2019-01-01`

**Download the information from all pages collected before:**
`handler.py download-pages --file "C:\Users\Jan\Desktop\football-transfers\tmp\all_pages.csv"`

**Scrape all information from the downloaded pages:**
`handler.py scrape-pages --file "C:\Users\Jan\Desktop\football-transfers\data\transfers2018.json"`