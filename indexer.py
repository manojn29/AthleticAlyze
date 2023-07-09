class PyLuceneIndexer:
    def search(self, query):
        results = []
        result = {'id': 'pylucene', 'user': 'pylucene','title': 'pylucene','snippet': 'pylucene'}
        results.append(result)
        return results

class BERTIndexer:
    def search(self, query):
        results = []
        result = {'id': 'bert', 'user': 'bert','title': 'bert','snippet': 'bert'}
        results.append(result)
        return results
