import torch
from transformers import BertModel, BertTokenizerFast
import numpy as np
import pandas as pd
import torch.nn.functional as F

def similarity(embeddings_1, embeddings_2):
    normalized_embeddings_1 = F.normalize(embeddings_1, p=2)
    normalized_embeddings_2 = F.normalize(embeddings_2, p=2)
    return torch.matmul(
        normalized_embeddings_1, normalized_embeddings_2.transpose(0, 1)
    )
    
class SemanticSimBa(object):
    """
    Semantic Similarity method that takes a batch of paired sentences and returns the matrix with the score appended
    """

    def __init__(self):
        """
        Add any initialization parameters. These will be passed at runtime from the graph definition parameters defined in your seldondeployment kubernetes resource manifest.
        """
        print("Initializing")
        self.tokenizer = self.get_tokenizer()
        self.model = self.get_model()

    @staticmethod
    def get_tokenizer():
        try:
            return BertTokenizerFast.from_pretrained('./tokenizer')
        except ValueError:
            return BertTokenizerFast.from_pretrained("setu4993/LaBSE")

    @staticmethod
    def get_model():
        try:
            model = torch.load('./model/model.pkl')
        except FileNotFoundError:
            model = BertModel.from_pretrained("setu4993/LaBSE")
        return model.eval()

    def predict(self, X, col_names=['sent1','sent2']):
        """
        Return a prediction.

        Parameters
        ----------
        X : array-like X[0] = sentence 1 X[1] = sentence 2
        col_names : array of feature names (optional)
        
        returns X plus scores
        """
        #print("Predict called - will run batch similarity function")
        df = pd.DataFrame(X, columns = col_names)
        df['sent1'] = df['sent1'].astype(str)
        df['sent2'] = df['sent2'].astype(str)

        sent1_inputs = self.tokenizer([df['sent1'][x] for x in range(0,len(df['sent1']))], return_tensors="pt", padding=True)
        sent2_inputs = self.tokenizer([df['sent2'][y] for y in range(0,len(df['sent2']))], return_tensors="pt", padding=True)

        with torch.no_grad():
            sent1_outputs = self.model(**sent1_inputs)
            sent2_outputs = self.model(**sent2_inputs)

        sent1_embeddings = sent1_outputs.pooler_output
        sent2_embeddings = sent2_outputs.pooler_output

        sim_matrix = similarity(sent1_embeddings, sent2_embeddings)*5

        df['sim score'] = [sim_matrix.numpy()[x][x] for x in range(0, sim_matrix.shape[0])] 
        X = df.to_numpy() 

        return X
