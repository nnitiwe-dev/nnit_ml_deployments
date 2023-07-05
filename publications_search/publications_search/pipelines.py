# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from datetime import datetime
from elasticsearch_dsl import Document, Date, Integer, Keyword, Text, connections

# Define the Elasticsearch connection
connections.create_connection(hosts=['54.172.6.64'])

class Article(Document):
    title = Text(analyzer='snowball', fields={'raw': Keyword()})
    authors = Keyword(multi=True)
    url = Keyword()
    abstract = Text(analyzer='snowball')

    class Index:
        name = 'researchpaper'
        settings = {
          "number_of_shards": 2,
        }


    def is_published(self):
        return datetime.now() > self.published_from

class ElasticsearchPipeline:
    def __init__(self):
        self.index_name = 'researchpaper'

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        article = Article()
        article.title = adapter.get('title')
        article.authors = adapter.get('authors')
        article.url = adapter.get('url')
        article.abstract = adapter.get('abstract')
        article.save(index=self.index_name)
        return item
