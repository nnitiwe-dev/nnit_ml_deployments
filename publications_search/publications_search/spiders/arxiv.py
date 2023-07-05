import scrapy
from publications_search.items import ArxivItem

class ArxivSpider(scrapy.Spider):
    name = 'arxiv'
    start_urls = ['https://arxiv.org/list/cs.AI/2307?skip=0&show=25']
    base_url = 'https://arxiv.org'
    pages=0

    def parse(self, response):
        self.logger.info(f'START: Crawling ARVIX for Articles- page {self.pages}')
        page_content=response.css('dl')

        articles = page_content.css('dd')
        urls = page_content.css('dt')
        for ind in range(len(articles)):
            item = ArxivItem()
            item['title'] = articles[ind].css('.list-title.mathjax').get().replace('<span class="descriptor">Title:</span> ', "").replace('<div class="list-title mathjax">', '').replace('</div>', '').strip()
            item['authors'] = [author.css('a::text').get() for author in articles[ind].css('.list-authors a')]
            item['url'] = self.base_url + urls[ind].css('.list-identifier a::attr(href)').get()

            yield scrapy.Request(item['url'], callback=self.parse_article, meta={'item': item})
        
        self.pages=self.pages+25
        next_page_url =response.url.split('?skip=')[0]+"?skip="+str(self.pages)+"&show=25" 
        
        #stop at page 5
        if next_page_url and self.pages<125:
            yield scrapy.Request(next_page_url, callback=self.parse)

    def parse_article(self, response):
        item = response.meta['item']
        item['abstract'] = response.css('.abstract.mathjax').get().replace('<span class="descriptor">Abstract:</span>  ','').replace('<blockquote class="abstract mathjax">','').replace('</blockquote>','').strip()
        yield item
