import scrapy
import csv
from urllib.parse import urljoin
from datetime import datetime, timedelta

class EconomicTimesSpider(scrapy.Spider):
    name = 'economic_times'

    def start_requests(self):
        base_url = 'https://economictimes.indiatimes.com/archivelist/year-2020,month-{month},starttime-{starttime}.cms'

        # Define the starttime ranges for each month in 2020
        month_starttimes = {
            1: range(43831, 43862),
            2: range(43862, 43891),
            3: range(43891, 43922),
            4: range(43922, 43952),
            5: range(43952, 43983),
            6: range(43983, 44013),
            7: range(44013, 44044),
            8: range(44044, 44075),
            9: range(44075, 44105),
            10: range(44105, 44136),
            11: range(44136, 44166),
            12: range(44166, 44197)
        }

        for month, start_times in month_starttimes.items():
            for start_time in start_times:
                url = base_url.format(month=month, starttime=start_time)
                yield scrapy.Request(url, callback=self.parse, cb_kwargs={'month': month, 'starttime': start_time})

    def parse(self, response, month, starttime):
        news_items = scrapy.Selector(text=response.text).css('ul.content li')

        data = []

        for news_item in news_items:
            headline = news_item.css('a::text').get()
            url = news_item.css('a::attr(href)').get()
            full_url = response.urljoin(url) if url else None

            data.append({
                'starttime': starttime,
                'month': month,
                'headline': headline.strip() if headline else None,
                'url': full_url
            })

        self.save_to_csv(data)

    def save_to_csv(self, data):
        with open('economic_times.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['starttime', 'month', 'headline', 'url']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Check if the file is empty and write the header row only if needed
            if csvfile.tell() == 0:
                writer.writeheader()

            writer.writerows(data)

if __name__ == '__main__':
    with open('economic_times.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['starttime', 'month', 'headline', 'url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

    process = scrapy.crawler.CrawlerProcess()
    process.crawl(EconomicTimesSpider)
    process.start()
