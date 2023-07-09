import os, csv
import pandas as pd
import faiss
from transformers import AutoTokenizer, AutoModel
import torch

# Read input from CSV file and create list of unique tweets
class bert_search:
    def __init__(self):
        self.tweets_row = []

        with open("march_final_csv_shuffled_1mb.csv", 'r', newline='') as file:
            reader = csv.reader(file)
            next(reader, None)
            for row in reader:
                self.tweets_row.append(row)

        print(len(self.tweets_row))


        self.index_loaded = faiss.read_index("bert_index.index")

        self.tokenizer = AutoTokenizer.from_pretrained('sentence-transformers/all-distilroberta-v1') # you can change the model here
        self.model = AutoModel.from_pretrained('sentence-transformers/all-distilroberta-v1')

    def convert_to_embedding(self, query):
        print("Convert query to embedding")
        tokens = {'input_ids': [], 'attention_mask': []}
        new_tokens = self.tokenizer.encode_plus(query, max_length=512,
                                           truncation=True, padding='max_length',
                                           return_tensors='pt')
        tokens['input_ids'].append(new_tokens['input_ids'][0])
        tokens['attention_mask'].append(new_tokens['attention_mask'][0])
        tokens['input_ids'] = torch.stack(tokens['input_ids'])
        tokens['attention_mask'] = torch.stack(tokens['attention_mask'])
        with torch.no_grad():
            outputs = self.model(**tokens)
        embeddings = outputs.last_hidden_state
        attention_mask = tokens['attention_mask']
        mask = attention_mask.unsqueeze(-1).expand(embeddings.size()).float()
        masked_embeddings = embeddings * mask
        summed = torch.sum(masked_embeddings, 1)
        summed_mask = torch.clamp(mask.sum(1), min=1e-9)
        mean_pooled = summed / summed_mask

        return mean_pooled[0] # assuming query is a single sentence


    def search(self, query):
        query_embedding = self.convert_to_embedding(query)

        D, I = self.index_loaded.search(query_embedding[None, :], 10)
        
        output = []
        print(len(I[0]))
        for i in range(len(I[0])):
            print("i:", i)
            print([D[0][i]], self.tweets_row[I[0][i]])
            ret = [D[0][i]] + self.tweets_row[I[0][i]]
            ret.pop(1)
            ret.pop(2)
            ret.pop(4)
            ret[4], ret[5] = ret[5], ret[4]
            output.append(ret)
        print("output:", output)
        return output

