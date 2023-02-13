import logging, sys, csv, re, shutil, time
# logging.disable(sys.maxsize)

import lucene
import os
from org.apache.lucene.store import MMapDirectory, SimpleFSDirectory, NIOFSDirectory
from java.nio.file import Paths
from org.apache.lucene.analysis.core import KeywordAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, TextField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.search import IndexSearcher, Query, ScoreDoc, TopDocs
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.index import FieldInfo, IndexWriter, IndexWriterConfig, IndexOptions, DirectoryReader

# lucene.initVM(vmargs=['-Djava.awt.headless=true'])
# Define the field names for the CSV file
class AthleticAlyze:
    TWEET_COL = "tweet"
    HASHTAG_COL = "hashtags"

    def __init__(self):
        # Initializing analyzers and parsers.
        self.tweetAnalyzer = StandardAnalyzer()
        self.hashtagAnalyzer = KeywordAnalyzer()

        self.tweetParser = QueryParser(self.TWEET_COL, self.tweetAnalyzer)
        self.hashtagParser = QueryParser(self.HASHTAG_COL, self.hashtagAnalyzer)

        # Definition of index directorys.
        self.indexDir = SimpleFSDirectory(Paths.get("AthleticAlyzeIndex"))

        # Creation of index writer.
        self.config = IndexWriterConfig(self.tweetAnalyzer)
        self.indexWriter = IndexWriter(self.indexDir, self.config)

    def index_tweets(self, fileName):
        # Method for indexing the csv file.
        start = time.time()
        # count = 0
        print("Indexing initialized..")
        with open(fileName, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                doc = Document()
                doc.add(TextField("id", row['id'], Field.Store.NO))
                doc.add(TextField(self.TWEET_COL, row['tweet'], Field.Store.YES))
                doc.add(TextField(self.HASHTAG_COL, row['hashtag'], Field.Store.YES))
                doc.add(TextField("latitude", row['lat'], Field.Store.NO))
                doc.add(TextField("longitude", row['long'], Field.Store.NO))
                self.indexWriter.addDocument(doc)

        self.indexWriter.commit()
        self.indexWriter.close()
        print("Indexing Completed.")
        end = time.time()
        print("Time taken for indexing:", end - start)

    def search_tweets(self, query_str, field):
        # Method for searching the keyword.
        query = None
        if(field == self.TWEET_COL):
            query = self.tweetParser.parse(query_str)
        elif field == self.HASHTAG_COL:
            query = self.hashtagParser.parse(query_str)
        else:
            print("Invalid column name")
            return

        searcher = IndexSearcher(DirectoryReader.open(self.indexDir))
        results = searcher.search(query, 10)

        print("Results:")
        for result in results.scoreDocs:
            output = searcher.doc(result.doc)
            print("Score:", result.score)
            print("Tweet:", output.get(self.TWEET_COL))
            print("Hashtags:", output.get(self.HASHTAG_COL))
            print("\n")


if __name__ == '__main__':
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    indexer = AthleticAlyze()
    fileName = sys.argv[1]
    indexer.index_tweets(fileName)
    queryStr = sys.argv[2]
    col = sys.argv[3]
    indexer.search_tweets(queryStr, col)