import pandas as pd
import numpy as np
from pathlib import Path
import random
from typing import Optional, Dict, List, Generator, Tuple
import time
import argparse
import pickle
import json

import clearml
import sys
sys.path.append('../../')
from word_alignment_steps import prepare_data

import torch
import torch.nn as nn
import torch.optim as optim

pd.set_option('display.max_rows', 500)


class Autoencoder(nn.Module):
    def __init__(self, in_size: int=41899, out_size: int=200, hidden_sizes: Optional[list]=None):
        super().__init__()
        self.hidden_sizes = hidden_sizes if hidden_sizes else []
        self.encoder_layers = nn.ModuleList()
        self.decoder_layers = nn.ModuleList()
        self.in_size = in_size
        self.out_size = out_size
        
        # Encoder
        for size in self.hidden_sizes:
            self.encoder_layers.append(nn.Linear(in_size, size))
            self.encoder_layers.append(nn.ReLU())
            in_size = size
        self.encoder_layers.append(nn.Linear(in_size, self.out_size))
        
        #Decoder
        for size in self.hidden_sizes[::-1]:
            self.decoder_layers.append(nn.Linear(out_size, size))
            self.decoder_layers.append(nn.ReLU())
            out_size = size
        self.decoder_layers.append(nn.Linear(out_size, self.in_size))

        self.encoder = nn.Sequential(*self.encoder_layers)
        self.decoder = nn.Sequential(*self.decoder_layers)
    
    def forward(self, x):
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)
        return decoded


def X_gen(word_dict: Dict[str, dict], languages: List[str], batch_size: int=32) -> Generator[torch.tensor, None, None]:
    """
    Generator that yields a tensor of the index_ohe arrays of batch size number of words.
    """
    dev = "cuda" if torch.cuda.is_available() else "cpu"
    for language in languages:
        words = list(word_dict[language].values())
        random.shuffle(words)
        for i in range(len(word_dict[language]) // batch_size):
            yield torch.tensor(np.array([word.index_ohe for word in words[i*batch_size:(i+1)*batch_size]])).float().to(dev)


def run_training(
                word_dict: Dict[str, Dict[str, prepare_data.Word]], 
                train_languages: List[str], 
                val_languages: List[str], 
                X_gen: Generator, 
                model: Autoencoder, 
                criterion, 
                optimizer: optim.Optimizer, 
                num_epochs: int, 
                batch_size: int=16) -> Tuple[Autoencoder, list]:
    """
    Runs training for the model, using the languages from train_languages, giving validation scores from val_languages.
    Words are in word_dict, which is fed into generator X_gen which yields batches of tensors. Criterion is loss function,
    optimizer, num_epochs and batch_size should be self-explanatory.

    Returns the final version of the model, as well as a list of the epoch training losses.
    """
    for lang in word_dict:
        for word in word_dict[lang].values():
            word.get_ohe()
    outputs = []
    for epoch in range(num_epochs):
        epoch_train_loss = np.array([])
        epoch_val_loss = np.array([])
        train_gen = X_gen(word_dict, train_languages, batch_size=batch_size)
        for batch_X in train_gen:
            optimizer.zero_grad()
            recon = model(batch_X)
            loss = criterion(recon, batch_X)
            epoch_train_loss = np.append(epoch_train_loss, loss.cpu().detach().numpy())
            loss.backward()
            optimizer.step()
        val_gen = X_gen(word_dict, val_languages, batch_size=batch_size) 
        for batch_val in val_gen:
            recon = model(batch_val)
            loss = criterion(recon, batch_val)
            epoch_val_loss = np.append(epoch_val_loss, loss.cpu().detach().numpy())
        print(f'Epoch:{epoch+1}, Train loss:{epoch_train_loss.mean():.6f}, Validation loss:{epoch_val_loss.mean():.6f}')
        clearml.Logger.current_logger().report_scalar("training/validation loss", "training loss", iteration=epoch+1, value=epoch_train_loss.mean())
        clearml.Logger.current_logger().report_scalar("training/validation loss", "validation loss", iteration=epoch+1, value=epoch_val_loss.mean())
        outputs.append((epoch, epoch_train_loss.mean()))
    return model, outputs

def train_model(
                word_dict: Dict[str, dict], 
                train_languages: List[str], 
                val_languages: List[str],
                generator: Generator, 
                in_size: int, 
                out_size: int,
                hidden_sizes: Optional[list]=None,
                loss_fn = nn.BCEWithLogitsLoss(), 
                num_epochs: int=100, 
                batch_size: int=128, 
                lr: float=0.001, 
                weight_decay: float=1e-7
                ) -> Tuple[Autoencoder, list]:
    """
    Sets up and runs a training run using the languages from train_languages, giving validation scores from val_languages.
    Words are in word_dict, which is fed into generator X_gen which yields batches of tensors. in_size, out_size and
    hidden_sizes are the number of nodes in the Autoencoder. loss_fn, num_epochs,  batch_size, lr (learning rate) and
    weight decay should be self-explanatory.

    Returns the final version of the model, as well as a list of the epoch training losses.
    """
    clearml.Task.add_requirements("./requirements.txt")
    clearml.Task.init(
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
    model = Autoencoder(in_size=in_size, out_size=out_size, hidden_sizes=hidden_sizes)
    model = model.cuda() if torch.cuda.is_available() else model
    optimizer = optim.Adam(model.parameters(), lr=lr, weight_decay=weight_decay)
    model, outputs = run_training(word_dict, train_languages, val_languages, generator, model, loss_fn, optimizer, num_epochs=num_epochs, batch_size=batch_size)
    return model, outputs


def create_index_cache(tokenized_df, refresh: bool = False):
    """
    Create an index cache from a tokenized DataFrame.

    Args:
        tokenized_df (pandas.DataFrame): A DataFrame where each row represents a source
        and target word in a verse
        refresh (bool, optional): If True, force a refresh of the cache. Defaults to False.

    Returns:
        dict: A dictionary that maps each word in the tokenized DataFrame to a set of
            verse indices where the word appears.
    """
    from word_alignment_steps import create_cache

    index_cache = create_cache.create_index_cache(tokenized_df)

    return index_cache


def get_index_cache(source: Path, cache_dir: Path, refresh: bool = False):
    """
    Get or create an index cache from a text file.

    Args:
        source (pathlib.Path): The path to the text file.
        cache_dir (pathlib.Path): The path to the directory where the index cache is / will be stored.
        refresh (bool, optional): If True, force a refresh of the cache. Defaults to False.

    Returns:
        dict: A dictionary that maps each word in the text file to a set of verse numbers where the
            word appears.

    Raises:
        FileNotFoundError: If the text file cannot be found.

    Notes:
        The index cache is stored in a JSON file in the specified cache directory.
    """
    
    with open(source) as f:
        # Read text file
        src_data = f.readlines()
    print(src_data[:20])
    index_cache_file = Path(f"{cache_dir}/{source.stem}-index-cache.json")
    (index_cache_file.parent).mkdir(parents=True, exist_ok=True)
    if index_cache_file.exists() and not refresh:
        print('Cache exists, loading...')
        with open(index_cache_file) as f:
            try:
                index_cache = json.load(f)
            except json.decoder.JSONDecodeError:
                print('Cache corrupted, creating...')
                tokenized_df = get_tokenized_df(src_data)

                index_cache = create_index_cache(tokenized_df, refresh=refresh)
                with open(index_cache_file, "w") as f:
                    json.dump(index_cache, f, indent=4)
    else:
        print('Cache does not exist, creating...')
        tokenized_df = get_tokenized_df(src_data)
        index_cache = create_index_cache(tokenized_df, refresh=refresh)
        with open(index_cache_file, "w") as f:
            json.dump(index_cache, f, indent=4)
    return index_cache


def get_tokenized_df(src_data: List[str]) -> pd.DataFrame:
    """
    Tokenize a list of verses (strings) and return a pandas DataFrame with the tokenized data.

    Args:
        src_data (List[str]): A list of verse strings to be tokenized.

    Returns:
        pandas.DataFrame: A DataFrame containing the tokenized data.

    Notes:
        This function uses the `prepare_data.create_tokens` function from the `prepare_data` module
        to tokenize the input strings. The resulting DataFrame has one row for each line of the
        input strings, and two columns: 'vref' (the verse reference) and 'src_tokenized' (a string
        of the tokenized words from that verse).
    """
    vref_filepath = Path("../vref.txt")
    df = pickle.loads(prepare_data.create_tokens(src_data, vref_filepath))
    return df


def main(args):
    outpath = Path(args.outpath)
    outpath.mkdir(parents=True, exist_ok=True)
    training_language_paths = {language.stem: language for language in args.train_langs}
    val_language_paths = {language.stem: language for language in args.val_langs}
    
    word_dict = {}
    for lang, lang_path in {**training_language_paths, **val_language_paths}.items():
        index_cache = get_index_cache(lang_path, args.cache_dir, refresh=args.refresh_cache)
        word_dict[lang] = prepare_data.get_words_from_cache(index_cache)
    
    model, outputs = train_model(
                                word_dict, 
                                training_language_paths.keys(),
                                val_language_paths.keys(),
                                X_gen, 
                                in_size=args.in_size, 
                                out_size=args.out_size,
                                hidden_sizes=args.hidden_sizes,
                                loss_fn=nn.BCEWithLogitsLoss(), 
                                num_epochs=args.num_epochs, 
                                batch_size=args.batch_size, 
                                lr=args.lr, 
                                weight_decay=args.weight_decay
                                )
    timestr = time.strftime("%Y%m%d-%H%M%S")
    torch.save(model.state_dict(), outpath / f'model_{len(training_language_paths)}_{timestr}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument('--train-langs', nargs='+', type=Path, help="A list of texts to train the model on.")
    parser.add_argument('--val-langs', nargs='+', type=Path, help="A list of texts to use for validation scores.")
    parser.add_argument('--cache-dir', type=Path, help="Path to the cache directory")
    parser.add_argument("--num-epochs", type=int, help="Number of epochs to train for", default=1)
    parser.add_argument("--batch-size", type=int, help="Batch size for training", default=128)
    parser.add_argument("--in-size", type=int, help="Input size", default=41899)
    parser.add_argument("--out-size", type=int, help="Output size", default=200)
    parser.add_argument("--hidden-sizes", nargs="+", type=int, help="Hidden sizes", default=None)
    parser.add_argument("--lr", type=float, help="Learning rate for training", default=0.001)
    parser.add_argument("--weight-decay", type=float, help="Weight decay for training", default=1e-6)
    parser.add_argument("--outpath", type=Path, help="where to store results")
    parser.add_argument("--refresh-cache", action='store_true', help="Refresh the cache of match scores")

    args, unknown = parser.parse_known_args()
    main(args)