
import scrapy
from typing import Generator

class DirectorySpider(scrapy.Spider):
    """
    V5 Directory Spider.
    Template for crawling association member lists / dictionaries.
    """
    name = "directory_spider"
    
    def __init__(self, start_urls=None, *args, **kwargs):
        super(DirectorySpider, self).__init__(*args, **kwargs)
        self.start_urls = start_urls or []

    def parse(self, response) -> Generator[dict, None, None]:
        # Example selector logic for a generic list
        # In production, this would be customized per source (adapter pattern)
        
        # 1. Extract Links to profiles
        for link in response.css('a::attr(href)').getall():
            if "member" in link or "company" in link:
                yield response.follow(link, self.parse_profile)
        
        # 2. Pagination
        next_page = response.css('a.next::attr(href)').get()
        if next_page:
            yield response.follow(next_page, self.parse)

    def parse_profile(self, response) -> Generator[dict, None, None]:
        yield {
            "source_url": response.url,
            "company": response.css('h1::text').get(),
            "website": response.css('a[href^="http"]::attr(href)').get(),
            "description": " ".join(response.css('p::text').getall()),
            "emails": response.xpath('//a[contains(@href, "mailto")]/@href').getall()
        }
