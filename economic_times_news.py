import scrapy
import pandas as pd
from datetime import datetime

class EconomicTimesSpider(scrapy.Spider):
    name = 'economic_times'
    header_written = False

    def start_requests(self):
        # Read the URLs from the CSV file
        urls_df = pd.read_csv('/content/scrapy_project/economic_times_spider/economic_times_spider/spiders/economic_times.csv')
        urls_list = urls_df['url'].tolist()

        for url in urls_list:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        # Extract the headline, summary, content, category of news, and URL
        headline = response.css('h1.artTitle::text').get()
        summary = response.css('h2.summary::text').get()
        content = ' '.join(response.css('div.artText ::text').getall()).strip()

        # Extract the category of news from breadcrumb
        category = response.css('div.clr.breadCrumb.contentwrapper span[itemprop="itemListElement"] a[title]::text').getall()
        category = ' › '.join(category).strip() if category else None

        # Extract the date and time of last update
        time_string = response.css('time.jsdtTime::attr(data-dt)').get()
        date_time = self.extract_date_time(time_string) if time_string else None

        # Log the extracted data
        self.logger.debug("Headline: %s", headline)
        self.logger.debug("Summary: %s", summary)
        self.logger.debug("Content: %s", content)
        self.logger.debug("Category: %s", category)
        self.logger.debug("Date and Time: %s", date_time)
        self.logger.debug("URL: %s", response.url)

        # Save the data to output.csv
        data = {
            'Headline': [headline],
            'Summary': [summary],
            'Content': [content],
            'Category': [category],
            'Date and Time': [date_time],
            'URL': [response.url]
        }

        df = pd.DataFrame(data)

        # Append to output.csv, write header only if not already written
        df.to_csv('/content/scrapy_project/economic_times_spider/output.csv', mode='a', index=False, header=not self.header_written)

        # Update the header_written flag
        self.header_written = True

    def extract_date_time(self, time_string):
        try:
            timestamp = int(time_string) / 1000
            dt = datetime.fromtimestamp(timestamp)
            return dt.strftime('%b %d, %Y, %I:%M %p %Z')
        except Exception as e:
            self.logger.error("Error extracting date and time: %s", e)
            return None
