import scrapy as sp
from scrapy.loader import ItemLoader
from scrapy101.items import Scrapy101Item

class QuotesSpider(sp.Spider):
    name='quotes'

    start_urls=['http://quotes.toscrape.com']


    def parse(self, response):
        self.logger.info('hello this is my first spider')
        
        quotes= response.css('div .quote')

        for quote in quotes:
            '''yield {
                'text': quote.css('.text::text').get(),
                'author':quote.css('.author::text').get(),
                'tags':quote.css('.tag::text').getall(),

            }'''

            loader=ItemLoader(item=Scrapy101Item(), selector=quote)

            loader.add_css('quote_content','.text::text')
            loader.add_css('tags','.tag::text')

            quote_item=loader.load_item()

            #get author details
            author_url= quote.css('.author + a::attr(href)').get()
            self.logger.info('getting Author page URL')

            yield response.follow(author_url, callback=self.parse_author,meta={'quote_item':quote_item})

        next_page=response.css('li.next a::attr(href)').get()

        if next_page is not None:
            next_page=response.urljoin(next_page)

            yield sp.Request(next_page, callback=self.parse)
    

    def parse_author(self, response):
        '''yield {
            'author_name':response.css('.author-title::text').get(),
            'author_birthday':response.css('.author-born-date::text').get(),
            'author_bornlocation': response.css('.author-born-location::text').get(),
            'author_bio':response.css('author-description::text').get(),
        }'''

        quote_item=response.meta['quote_item']
        loader=ItemLoader(item=quote_item, response=response)
        loader.add_css('author_name','.author-title::text')
        loader.add_css('author_birthday','.author-born-date::text')
        loader.add_css('author_bornlocation','.author-born-location::text')
        loader.add_css('author_bio','.author-description::text')
        yield loader.load_item()