from flask import Flask, render_template, request
from indexer import PyLuceneIndexer, BERTIndexer
from AthleticAlyze import AthleticAlyze
from bert_Searcher import bert_search
import os
import shutil

app = Flask(__name__)

@app.before_first_request
def initialize():
    print("Flask app is starting...")
    bertObj = bert_search()
    app.config['bertObj'] = bertObj

@app.route('/',methods = ['GET','POST'])
def index():
    try:
        shutil.rmtree('AthleticAlyzeIndex')
        print("Directory has been removed successfully")
    except:
        # print(error)
        print("Directory can not be removed")
    return render_template('index.html')

@app.route('/map', methods = ['GET','POST'])
def map():
    return render_template('mapnew.html')


@app.route('/search', methods=['POST'])
def search():
    # print("Search")
    query = request.form['query']
    print("query:", query)
    index_choice = request.form['index']
    indexer = get_indexer(index_choice, query)
    # results = indexer.search(query)
    return render_template('results.html', results=indexer)


# Initialize the appropriate indexer based on user input
def get_indexer(index_choice, query):
    if index_choice == "pylucene":
        # lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        print("pylucene")
        print("query:", query, "choice:", index_choice)
        obj = AthleticAlyze()
        obj.index_tweets("march_final_csv_shuffled_1mb.csv")
        result = obj.search_tweets(query, "tweet")
        return result
        # return PyLuceneIndexer()
    elif index_choice == "bert":
        bertObj = app.config['bertObj']
        return bertObj.search(query)
        # return BERTIndexer()


if __name__ == '__main__':
    app.run(debug=True,port = 5000)
