import pandas as pd
import numpy as np
from tqdm import tqdm
from pathlib import Path
import random
from typing import Iterable, Optional, Dict, List
import time
import argparse

import clearml

import torch
import torch.nn as nn
import torch.optim as optim

from match import get_correlations_between_sets, initialize_cache, get_combined_df, write_dictionary_to_file
pd.set_option('display.max_rows', 500)


def create_words(language_paths: Dict[str, Path], index_cache_paths, outpath, refresh_cache: bool=False):
    index_lists = {}
    word_dict = {}
    index_cache_files = {} 
    for language in language_paths:
        index_cache_files[language] = index_cache_paths[language] / f'{language}-index-cache.json'
        if index_cache_files[language].exists() and not refresh_cache:
            index_lists[language] = initialize_cache(index_cache_files[language], refresh=False)
            word_dict[language] = {word: Word(word) for word in index_lists[language]}
            for word in word_dict[language].values():
                word.index_list = index_lists[language][word.word]
                word.get_ohe()
        else:
            index_cache_files[language].parent.mkdir(parents=True, exist_ok=True)
            print(f"Getting sentences that contain each word in {language}")
            ref_df = get_combined_df(language_paths[language], language_paths[language], outpath)
            word_dict[language] = {word: Word(word) for word in ref_df['target'].explode().unique()}
            for word in tqdm(word_dict[language].values()):
                word.get_indices(ref_df['target'])
                word.get_ohe()
            index_lists[language] = {word.word: word.index_list for word in word_dict[language].values()}
            write_dictionary_to_file(index_lists[language], index_cache_files[language])
    return word_dict

class Word():
    def __init__(self, word: str):
        self.word = word
        self.matched = []
        self.index_list = np.array([])
        self.index_ohe = np.array([])
        self.norm_ohe = np.array([])
        self.encoding = np.array([])
    
    def get_indices(self, list_series):
        self.index_list = list(list_series[list_series.apply(lambda x: self.word in x if isinstance(x, Iterable) else False)].index)

    def get_matches(self, word):
        jac_sim, count = get_correlations_between_sets(set(self.index_list), set(word.index_list))
        return (jac_sim, count)
    
    def get_encoding(self, model):
        self.encoding = model.encoder(torch.tensor(self.index_ohe).float()).detach().numpy()        
    
    def get_ohe(self, max_num=41899):
        a = np.zeros(max_num)
        np.put(a, self.index_list, 1)    
        self.index_ohe = a
                       
    def get_norm_ohe(self, max_num=41899):
        a = np.zeros(max_num)
        np.put(a, self.index_list, 1)  
        norm_a = a / np.linalg.norm(a)
        self.norm_ohe = norm_a
        
    def get_distance(self, word):
        if word.encoding is None or self.encoding is None:
            return
        distance = np.linalg.norm(self.encoding - word.encoding)
        return distance
    
    def get_norm_distance(self, word):
        if word.encoding.shape != self.encoding.shape:
            return
        self.norm_encoding = self.encoding / np.linalg.norm(self.encoding)
        word.norm_encoding = word.encoding / np.linalg.norm(word.encoding)
        distance = np.linalg.norm(self.norm_encoding - word.norm_encoding)
        return distance


class Autoencoder(nn.Module):
    def __init__(self):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(41899, 200),
            nn.ReLU(),
            nn.Linear(200, 200),
        )
        self.decoder = nn.Sequential(
            nn.Linear(200, 200),
            nn.ReLU(),
            nn.Linear(200, 41899),
            # nn.Sigmoid(),
        )
        
    def forward(self, x):
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)
        return decoded

def X_gen(word_dict, languages, batch_size=32):
    for language in languages:
        words = list(word_dict[language].values())
        random.shuffle(words)
        for i in range(len(word_dict[language]) // batch_size):
            yield torch.tensor(np.array([word.index_ohe for word in words[i*batch_size:(i+1)*batch_size]])).float()  #.to(torch.device(dev))


def run_training(word_dict: Dict[str, Dict[str, Word]], languages: List[str], X_gen, model, criterion, optimizer, num_epochs, batch_size=16):
    outputs = []
    for epoch in range(num_epochs):
        epoch_loss = np.array([])
        gen = X_gen(word_dict, languages, batch_size=batch_size)
        for batch_X in gen:
            optimizer.zero_grad()
            recon = model(batch_X)
            loss = criterion(recon, batch_X)
            epoch_loss = np.append(epoch_loss, loss.detach().numpy())
            loss.backward()
            optimizer.step()
        print(f'Epoch:{epoch+1}, Loss:{epoch_loss.mean():.6f}')
        outputs.append((epoch, epoch_loss.mean()))
    return model, outputs

def train_model(word_dict, languages, generator, loss_fn=nn.BCELoss(), num_epochs=100, batch_size=128, lr=0.001, weight_decay=1e-7):
    clearml.Task.add_requirements("./requirements.txt")
    task = clearml.Task.init(
      project_name='Word-alignment-autoencoder',    # project name of at least 3 characters
      task_name='autoencoder-train-' + str(int(time.time())), # task name of at least 3 characters
      task_type="training",
      tags=None,
      reuse_last_task_id=True,
      continue_last_task=False,
      output_uri="s3://aqua-word-alignment",
      auto_connect_arg_parser=True,
      auto_connect_frameworks=True,
      auto_resource_monitoring=True,
      auto_connect_streams=True,    
    )
    model = Autoencoder()
    # loss_fn = nn.BCELoss()
    loss_fn = nn.BCEWithLogitsLoss()
    optimizer = optim.Adam(model.parameters(), lr=lr, weight_decay=weight_decay)
    model, outputs = run_training(word_dict, languages, generator, model, loss_fn, optimizer, num_epochs=num_epochs, batch_size=batch_size)
    return model, outputs


def load_model(filepath: Path) -> Autoencoder:
    model = Autoencoder()
    model.load_state_dict(torch.load(filepath))
    return model


def get_encodings(word_dict_lang:Dict[str,Word], model: Autoencoder) -> None:
    for word in tqdm(word_dict_lang.values()):
            word.get_encoding(model)

        
def add_distances_to_df(source: Path, target: Path, outpath: Path, model: Autoencoder, df: Optional[pd.DataFrame]=None, cache_path: Optional[Path]=None, refresh_cache: bool=False) -> None:
    if not df:
        df = pd.read_csv(outpath / 'all_in_context_with_scores.csv')
    cache_path = cache_path if cache_path else outpath / 'cache'
    language_paths = {language_path.stem: language_path for language_path in [source, target]}
    index_cache_paths = {}
    for language in language_paths:
        index_cache_paths[language] = cache_path
    word_dict = create_words(language_paths, index_cache_paths, outpath, refresh_cache=refresh_cache)
    for language in language_paths:
        print(f"Getting {language} encodings")
        get_encodings(word_dict[language], model)
    print("Adding encoding distances to the data")
    df.loc[:, 'encoding_dist'] = df.progress_apply(lambda row: word_dict[source.stem].get(row['source'], Word('')).get_norm_distance(word_dict[target.stem].get(row['target'], Word(''))), axis=1)
    df.to_csv(outpath / 'all_in_context_with_scores.csv')


def main(args):
    outpath = args.outpath
    outpath.mkdir(parents=True, exist_ok=True)
    # cache_dir = outpath / "cache"
    # cache_dir.mkdir(exist_ok=True)
    training_language_paths = {language.stem: language for language in args.train_langs}
#                 'en-NLT07', 
#                 'fr-LBS21', 
#                 'swh-ONEN',
#                 'es-NTV',
    # languages = [args.source.stem, args.target.stem, *training_languages]
    index_cache_paths = {}
    for language in training_language_paths:
        index_cache_paths[language] = outpath  / 'cache'
    word_dict = create_words(training_language_paths, index_cache_paths, outpath, refresh_cache=args.refresh_cache)
    model, outputs = train_model(word_dict, training_language_paths, X_gen, loss_fn=nn.BCELoss(), num_epochs=args.num_epochs, batch_size=args.batch_size, lr=args.lr, weight_decay=args.weight_decay)
    timestr = time.strftime("%Y%m%d-%H%M%S")
    torch.save(model.state_dict(), outpath / f'model_{len(training_language_paths)}_{timestr}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument('--train-langs', nargs='+', type=Path, help="A list of texts to train the model on."
        )
    parser.add_argument("--num-epochs", type=int, help="Number of epochs to train for", default=1)
    parser.add_argument("--batch-size", type=int, help="Batch size for training", default=128)
    parser.add_argument("--lr", type=float, help="Learning rate for training", default=0.001)
    parser.add_argument("--weight-decay", type=float, help="Weight decay for training", default=1e-6)
    parser.add_argument("--outpath", type=Path, help="where to store results")
    parser.add_argument("--refresh-cache", action='store_true', help="Refresh the cache of match scores")

    args, unknown = parser.parse_known_args()
    main(args)