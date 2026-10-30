import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime


def count_yle_articles(query, start_date, end_date):
    search_query = (
        f"site:yle.fi/a/ ({query}) "
        f"after:{start_date} before:{end_date}"
    )

    url = (
        "https://news.google.com/rss/search?q="
        + quote(search_query)
        + "&hl=fi&gl=FI&ceid=FI:fi"
    )

    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    response.raise_for_status()

    root = ET.fromstring(response.content)

    urls = {
        item.findtext("link")
        for item in root.findall(".//item")
        if item.findtext("link")
    }

    return len(urls)

def get_month(month):
    if month > 12:
        return "01",1
    if month > 9:
        return str(month),0
    return "0" + str(month),0

if __name__ == "__main__":
    start_year = 2001
    end_year = 2007
    query = '"ukraina" OR "ukrainassa'

    yearly_total = []
    monthly_dates = []
    monthly_totals = []

    for year in range(end_year - start_year + 1):
        yearly_total.append(0)
        for month in range(12):
            start_date = f"{start_year + year}-{get_month(month + 1)[0]}-01"
            end_date = f"{start_year + year + get_month(month + 2)[1]}-{get_month(month + 2)[0]}-01"

            print(start_date)
            print(end_date)


            total = count_yle_articles(
                query=query,
                start_date=start_date,
                end_date=end_date,
            )

            monthly_totals.append(total)
            monthly_dates.append(f"{get_month(month+1)[0]}-{start_year + year}")

            print(f"Monthly articles {year + start_year}: {total}")
            yearly_total[year] += total

    for index, total in enumerate(yearly_total):
        print(f"{index + start_year} total: {total}")

    fig, ax = plt.subplots(figsize=(15, 6))

    dates = [datetime.strptime(d, "%m-%Y") for d in monthly_dates]

    years = list(range(start_year,end_year + 1))

    print(len(years))
    print(len(yearly_total))

    ax.plot(
        years,
        yearly_total
    )

    ax.set_xlabel("Aika")
    ax.set_ylabel("Uutiset")
    #ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    #ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))

    plt.xticks(rotation=45, ha="right")

    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    plt.tight_layout()

    plt.savefig("yle.png", dpi=200)
    plt.show()

    print("Graph saved to yle.png")
    for date in monthly_dates:
        print(date)
