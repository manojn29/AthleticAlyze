#Indexer_backup

from transformers import AutoTokenizer, AutoModel
import torch
import csv
import pandas as pd
import faiss
import time

t1 = time.time()
tokenizer = AutoTokenizer.from_pretrained('sentence-transformers/all-distilroberta-v1') # you can change the model here
model = AutoModel.from_pretrained('sentence-transformers/all-distilroberta-v1')

# Read input from CSV file and create list of unique tweets\
tweet_rows = []
tweet_body = []
batch_size = 5

with open("march_final_csv_shuffled_1mb.csv", 'r', newline='') as file:
    reader = csv.reader(file)
    next(reader, None)
    for row in reader:
        tweet_rows.append(row)
        tweet_body.append(str(row[1]).lower())

batched_tweets = [tweet_body[i:i + batch_size] for i in range(0, len(tweet_body), batch_size)]
# print(len(unique_tweets))
print(len(tweet_rows))
print(len(tweet_body))

index = faiss.IndexFlatIP(768)   # build Faiss index

#Process Tweets in batch

def tweets_to_embeddings(tokens):
   
    tokens['input_ids'] = torch.stack(tokens['input_ids'])
    tokens['attention_mask'] = torch.stack(tokens['attention_mask'])
    with torch.no_grad():
        outputs = model(**tokens)
    embeddings = outputs.last_hidden_state
    attention_mask = tokens['attention_mask']
    mask = attention_mask.unsqueeze(-1).expand(embeddings.size()).float()
    masked_embeddings = embeddings * mask
    summed = torch.sum(masked_embeddings, 1)
    summed_mask = torch.clamp(mask.sum(1), min=1e-9)
    mean_pooled = summed / summed_mask
    return mean_pooled


tokens = {'input_ids': [], 'attention_mask': []}
batch_id = 0

for batch in batched_tweets:
    for tweet in batch:
        # encode each tweet and append to dictionary
        new_tokens = tokenizer.encode_plus(tweet, max_length=512,
                                        truncation=True, padding='max_length',
                                        return_tensors='pt')
        tokens['input_ids'].append(new_tokens['input_ids'][0])
        tokens['attention_mask'].append(new_tokens['attention_mask'][0])

    batch_id += 1
    embeddings = tweets_to_embeddings(tokens)
    index.add(embeddings)
    faiss.write_index(index,"Latest_index/batch_" + str(batch_id) + ".index")
    print("Idx : " + str(batch_id))
    if(batch_id % 200 == 0):
        print("time:", time.time() - t1)
    tokens = {'input_ids': [], 'attention_mask': []}

print("Faiss Index size : " + str(index.ntotal))
# Query conversion

def convert_to_embedding(query):
    print("Convert query to embedding")
    tokens = {'input_ids': [], 'attention_mask': []}
    new_tokens = tokenizer.encode_plus(query, max_length=512,
                                       truncation=True, padding='max_length',
                                       return_tensors='pt')
    tokens['input_ids'].append(new_tokens['input_ids'][0])
    tokens['attention_mask'].append(new_tokens['attention_mask'][0])
    tokens['input_ids'] = torch.stack(tokens['input_ids'])
    tokens['attention_mask'] = torch.stack(tokens['attention_mask'])
    with torch.no_grad():
        outputs = model(**tokens)
    embeddings = outputs.last_hidden_state
    attention_mask = tokens['attention_mask']
    mask = attention_mask.unsqueeze(-1).expand(embeddings.size()).float()
    masked_embeddings = embeddings * mask
    summed = torch.sum(masked_embeddings, 1)
    summed_mask = torch.clamp(mask.sum(1), min=1e-9)
    mean_pooled = summed / summed_mask
    
    return mean_pooled[0] # assuming query is a single sentence


query = "messi"
query_embedding = convert_to_embedding(query)

D, I = index.search(query_embedding[None, :], 5) # None dimension is added because we only have one query against 4 documents

for id in I[0] :
    print(tweet_rows[id])


faiss.write_index(index,"bert_index.index")
index_loaded = faiss.read_index("bert_index.index")

print("After loading previous index from index file.")
D, I = index_loaded.search(query_embedding[None, :], 5)
t2 = time.time()
print("Time elapsed:", t2 - t1)