import logging, sys, csv, re, shutil, time
# logging.disable(sys.maxsize)
import jpype
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
# from org.apache.lucene.spatial3d.geom import GeoPoint, Geo3DPoint, PlanetModel, Geo3DCircle, LatLonBounds, LatLonPoint
import csv


# Define the field names for the CSV file
class AthleticAlyze:

    def __init__(self):
        
        try:
            lucene.initVM(vmargs=['-Djava.awt.headless=false'])
        except:
            vm_env = lucene.getVMEnv()
            vm_env.attachCurrentThread()
            print("JVM is already running")
        self.TWEET_COL = "tweet"
        self.HASHTAG_COL = "hashtags"
        self.tweetAnalyzer = StandardAnalyzer()
        self.hashtagAnalyzer = KeywordAnalyzer()

        self.tweetParser = QueryParser(self.TWEET_COL, self.tweetAnalyzer)
        self.hashtagParser = QueryParser(self.HASHTAG_COL, self.hashtagAnalyzer)
        self.latParser = QueryParser(self.HASHTAG_COL, self.hashtagAnalyzer)
        self.longParser = QueryParser(self.HASHTAG_COL, self.hashtagAnalyzer)
        self.urlParser = QueryParser(self.HASHTAG_COL, self.hashtagAnalyzer)

        # Definition of index directorys.
        self.indexFolder = SimpleFSDirectory(Paths.get("AthleticAlyzeIndex"))

        # Creation of index writer.
        self.config = IndexWriterConfig(self.tweetAnalyzer)
        self.indexWriter = IndexWriter(self.indexFolder, self.config)

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
                doc.add(TextField("lat", str(row['lat']), Field.Store.YES))
                doc.add(TextField("long", str(row['long']), Field.Store.YES))
                doc.add(TextField("tweet_url", str(row['tweet_url']), Field.Store.YES))
                # doc.add(LatLonPoint("location", float(row['lat']), float(row['long'])))
                self.indexWriter.addDocument(doc)

        self.indexWriter.commit()
        self.indexWriter.close()
        print("Indexing Completed.")
        end = time.time()
        print("Time taken for indexing:", end - start)

    def search_tweets(self, query_str, field):
        # Method for searching the keyword.
        csv_file_name= "lat_long.csv"
        query = None
        
        if(field == self.HASHTAG_COL):
            query = self.hashtagParser.parse(query_str)
        elif(field == self.TWEET_COL):
            query = self.tweetParser.parse(query_str)
        elif(field == 'lat'):
            query = self.latParser.parse(query_str)
        elif(field == 'long'):
            query = self.longParser.parse(query_str)
        elif(field == 'tweet_url'):
            query = self.longParser.parse(query_str)
        else:
            print("Invalid column name. Please provide either 'tweet' or 'hashtags' as 3rd parameter.")
            return

        searcher = IndexSearcher(DirectoryReader.open(self.indexFolder))
        searchResults = searcher.search(query, 20)

        print("Results:\n")
        ids = set()
        retVal = []
        for result in searchResults.scoreDocs:
            output = searcher.doc(result.doc)
            if str(output.get(self.TWEET_COL)) not in ids:
                output = searcher.doc(result.doc)
                retVal.append([str(result.score), output.get(self.TWEET_COL), str(output.get('lat')), str(output.get('long')), output.get(self.HASHTAG_COL), output.get('tweet_url')])
            ids.add(output.get(self.TWEET_COL))
                
        return retVal
    
    

# if __name__ == '__main__':
#     lucene.initVM(vmargs=['-Djava.awt.headless=true'])
#     indexer = AthleticAlyze()
#     # fileName = sys.argv[1]
#     fileName = "final_onemb.csv"
#     indexer.index_tweets(fileName)
#     queryStr = sys.argv[2]
#     col = sys.argv[3]
#     indexer.search_tweets(queryStr, col)
