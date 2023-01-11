import argparse
from pathlib import Path
import sys
import os
import json

import modal
import boto3
import pandas as pd

dir = (Path(__file__).parent.parent.parent / "pipelines/scripts").as_posix()
sys.path.append(dir)
import create_cache.create_cache as create_cache
import alignment_scores.alignment_scores as alignment_scores
import translation_scores.translation_scores as translation_scores
import match_contexts.match_contexts as match_contexts
import embeddings.embeddings as embeddings
import total_scores.total_scores as total_scores
import top_source_scores.top_source_scores as top_source_scores
import create_refs.create_refs as create_refs
import verse_scores.verse_scores as verse_scores
import red_flags.red_flags as red_flags
import threshold_scores.threshold_scores as threshold_scores
import common_files.get_data as get_data


local_data_dir = Path("../../pipelines/scripts/word_alignment/data")
remote_data_dir = Path("/data/")


def does_file_exist(filepath):
    bucket = "aqua-word-alignment"
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=filepath.as_posix())
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
    else:
        print(f"{filepath} already exists in {bucket} S3 bucket")
        return True


def write_df_to_s3(df: pd.DataFrame, filepath: Path):
    df_csv = df.to_csv(index=False)
    s3 = boto3.client("s3")
    s3.put_object(Bucket="aqua-word-alignment", Key=filepath.as_posix(), Body=df_csv)


def write_dict_to_s3(dict_file: dict, filepath: Path):
    json_data = json.dumps(dict_file, ensure_ascii=False, indent=4)
    s3 = boto3.client("s3")
    s3.put_object(Bucket="aqua-word-alignment", Key=filepath.as_posix(), Body=json_data)


def create_meta_file(source, target):
    meta = {}
    meta["source"] = source.stem
    meta["target"] = target.stem
    write_dict_to_s3(
        meta, Path(f"Modal/out/meta_files/{source.stem}_{target.stem}/meta.json")
    )


def upload_texts(source, target):
    s3 = boto3.client("s3")
    s3.upload_file(source, "aqua-word-alignment", f"Modal/out/texts/{source.name}")
    s3.upload_file(target, "aqua-word-alignment", f"Modal/out/texts/{target.name}")


def create_index_cache(source, outpath, is_bible: bool = True, refresh: bool = False):
    index_cache_file = Path(f"Modal/cache/{source.stem}-index-cache.json")
    if does_file_exist(index_cache_file) and not refresh:
        return
    word_dict = create_cache.create_index_cache(source, outpath, is_bible=is_bible)
    index_cache = {word.word: word.index_list for word in word_dict.values()}
    write_dict_to_s3(index_cache, Path(f"Modal/cache/{source.stem}-index-cache.json"))


def create_alignment_scores(
    source, target, outpath, is_bible: bool = True, refresh: bool = False
):
    if (
        does_file_exist(
            Path(
                f"Modal/out/alignment_scores/{source.stem}_{target.stem}/alignment_scores.csv"
            )
        )
        and not refresh
    ):
        return
    df, avg_df, parallel_corpus, symmetrized_model = alignment_scores.run_best_align(
        source, target, outpath, is_bible=is_bible
    )
    write_df_to_s3(
        df,
        Path(
            f"Modal/out/alignment_scores/{source.stem}_{target.stem}/alignment_scores.csv"
        ),
    )
    write_df_to_s3(
        avg_df,
        Path(
            f"Modal/out/alignment_scores/{source.stem}_{target.stem}/avg_alignment_scores.csv"
        ),
    )


def create_translation_scores(
    source, target, outpath, is_bible: bool = True, refresh: bool = False
):
    if (
        does_file_exist(
            Path(
                f"Modal/out/translation_scores/{source.stem}_{target.stem}/translation_scores.csv"
            )
        )
        and not refresh
    ):
        return
    df, _, _ = translation_scores.run_translation_scores(
        source, target, outpath, is_bible=is_bible
    )
    write_df_to_s3(
        df,
        Path(
            f"Modal/out/translation_scores/{source.stem}_{target.stem}/translation_scores.csv"
        ),
    )


def create_match_scores(
    source, target, outpath, is_bible: bool = True, refresh: bool = False
):
    if (
        does_file_exist(
            Path(f"Modal/out/match_scores/{source.stem}_{target.stem}/dictionary.json")
        )
        and not refresh
    ):
        return
    s3 = boto3.client("s3")
    source_index_cache_file = f"{source.stem}-index-cache.json"
    target_index_cache_file = f"{target.stem}-index-cache.json"
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/cache/{source_index_cache_file}",
        source_index_cache_file,
    )
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/cache/{target_index_cache_file}",
        target_index_cache_file,
    )

    matches, _, _ = match_contexts.run_match(
        source,
        target,
        outpath,
        source_index_cache_file=Path(source_index_cache_file),
        target_index_cache_file=Path(target_index_cache_file),
    )
    write_dict_to_s3(
        matches,
        Path(f"Modal/out/match_scores/{source.stem}_{target.stem}/dictionary.json"),
    )


def create_embeddings(
    source, target, outpath, is_bible: bool = True, refresh: bool = False
):
    if (
        does_file_exist(
            Path(
                f"Modal/out/embedding_scores/{source.stem}_{target.stem}/embedding_scores.csv"
            )
        )
        and not refresh
    ):
        return
    s3 = boto3.client("s3")
    source_index_cache_file = f"{source.stem}-index-cache.json"
    target_index_cache_file = f"{target.stem}-index-cache.json"
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/cache/{source_index_cache_file}",
        source_index_cache_file,
    )
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/cache/{target_index_cache_file}",
        target_index_cache_file,
    )

    df = embeddings.get_embeddings(
        source,
        target,
        outpath,
        source_index_cache_file=Path(source_index_cache_file),
        target_index_cache_file=Path(target_index_cache_file),
    )
    write_df_to_s3(
        df,
        Path(
            f"Modal/out/embedding_scores/{source.stem}_{target.stem}/embedding_scores.csv"
        ),
    )


def create_total_scores(
    source, target, outpath, is_bible: bool = True, refresh: bool = False
):
    if (
        does_file_exist(
            Path(f"Modal/out/total_scores/{source.stem}_{target.stem}/total_scores.csv")
        )
        and not refresh
    ):
        return
    s3 = boto3.client("s3")
    alignment_scores_file = Path("alignment_scores.csv")
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/out/alignment_scores/{source.stem}_{target.stem}/alignment_scores.csv",
        alignment_scores_file,
    )
    alignment_scores = pd.read_csv(alignment_scores_file)
    avg_alignment_scores = Path("avg_alignment_scores.csv")
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/out/alignment_scores/{source.stem}_{target.stem}/avg_alignment_scores.csv",
        avg_alignment_scores,
    )
    avg_alignment_scores = pd.read_csv(avg_alignment_scores)
    translation_scores_file = Path("translation_scores.csv")
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/out/translation_scores/{source.stem}_{target.stem}/translation_scores.csv",
        translation_scores_file,
    )
    translation_scores = pd.read_csv(translation_scores_file)
    match_scores_file = Path("match_scores.csv")
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/out/match_scores/{source.stem}_{target.stem}/dictionary.json",
        match_scores_file,
    )
    with open(match_scores_file) as f:
        match_scores = json.load(f)
    embedding_scores_file = Path("embedding_scores.csv")
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/out/embedding_scores/{source.stem}_{target.stem}/embedding_scores.csv",
        embedding_scores_file,
    )
    embedding_scores = pd.read_csv(embedding_scores_file)
    df = total_scores.run_total_scores(
        source,
        target,
        alignment_scores,
        avg_alignment_scores,
        translation_scores,
        match_scores,
        embedding_scores,
    )
    write_df_to_s3(
        df, Path(f"Modal/out/total_scores/{source.stem}_{target.stem}/total_scores.csv")
    )


def create_top_source_scores(
    source, target, outpath, is_bible: bool = True, refresh: bool = False
):
    if (
        does_file_exist(
            Path(
                f"Modal/out/top_source_scores/{source.stem}_{target.stem}/top_source_scores.csv"
            )
        )
        and not refresh
    ):
        return
    s3 = boto3.client("s3")
    total_scores_file = Path("total_scores.csv")
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/out/total_scores/{source.stem}_{target.stem}/total_scores.csv",
        total_scores_file,
    )
    total_scores = pd.read_csv(total_scores_file)
    df = top_source_scores.get_verse_scores(total_scores)
    write_df_to_s3(
        df,
        Path(
            f"Modal/out/top_source_scores/{source.stem}_{target.stem}/top_source_scores.csv"
        ),
    )


def create_ref_scores(
    source, target, outpath, is_bible: bool = True, refresh: bool = False
):
    if does_file_exist(Path(f"Modal/out/refs/{source.stem}-refs.csv")) and not refresh:
        return
    s3 = boto3.client("s3")
    top_source_scores_file = Path("top_source_scores.csv")
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/out/top_source_scores/{source.stem}_{target.stem}/top_source_scores.csv",
        top_source_scores_file,
    )
    top_source_scores = pd.read_csv(top_source_scores_file)
    verse_scores_file = Path("verse_scores.csv")
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/out/verse_scores/{source.stem}_{target.stem}/verse_scores.csv",
        verse_scores_file,
    )
    verse_scores = pd.read_csv(verse_scores_file)
    if does_file_exist(Path(f"Modal/out/refs/{source.stem}-refs.csv")):
        ref_file = Path("refs.csv")
        s3.download_file(
            "aqua-word-alignment", f"Modal/out/refs/{source.stem}-refs.csv", ref_file
        )
        ref_scores = pd.read_csv(ref_file)
    else:
        ref_scores = create_refs.create_new_ref_df(source)
    if does_file_exist(Path(f"Modal/out/refs/{source.stem}-verse-refs.csv")):
        ref_verse_file = Path("refs.csv")
        s3.download_file(
            "aqua-word-alignment",
            f"Modal/out/refs/{source.stem}-verse-refs.csv",
            ref_verse_file,
        )
        ref_verse_scores = pd.read_csv(ref_verse_file)
    else:
        ref_verse_scores = create_refs.create_new_ref_df(source)
        ref_verse_scores = ref_verse_scores.groupby(
            "vref", sort=False, as_index=False
        ).first()[["vref"]]

    df = create_refs.add_scores_to_ref(target.stem, top_source_scores, ref_scores)
    write_df_to_s3(df, Path(f"Modal/out/refs/{source.stem}-refs.csv"))
    df = create_refs.add_verse_scores_to_ref(
        target.stem, verse_scores, ref_verse_scores
    )
    write_df_to_s3(df, Path(f"Modal/out/refs/{source.stem}-verse-refs.csv"))


def create_verse_scores(
    source, target, outpath, is_bible: bool = True, refresh: bool = False
):
    if (
        does_file_exist(
            Path(f"Modal/out/verse_scores/{source.stem}_{target.stem}/verse_scores.csv")
        )
        and not refresh
    ):
        return
    s3 = boto3.client("s3")
    top_source_scores_file = Path("top_source_scores.csv")
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/out/top_source_scores/{source.stem}_{target.stem}/top_source_scores.csv",
        top_source_scores_file,
    )
    top_source_scores = pd.read_csv(top_source_scores_file)
    df = verse_scores.get_verse_scores(top_source_scores)
    write_df_to_s3(
        df, Path(f"Modal/out/verse_scores/{source.stem}_{target.stem}/verse_scores.csv")
    )


def create_red_flags(
    source, target, outpath, is_bible: bool = True, refresh: bool = False
):
    if (
        does_file_exist(
            Path(
                f"Modal/out/red_flags/{source.stem}_{target.stem}/possible_red_flags.csv"
            )
        )
        and not refresh
    ):
        return
    s3 = boto3.client("s3")
    top_source_scores_file = Path("top_source_scores.csv")
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/out/top_source_scores/{source.stem}_{target.stem}/top_source_scores.csv",
        top_source_scores_file,
    )
    top_source_scores = pd.read_csv(top_source_scores_file)
    refs_file = Path("ref_scores.csv")
    s3.download_file(
        "aqua-word-alignment", f"Modal/out/refs/{source.stem}-refs.csv", refs_file
    )
    refs = pd.read_csv(refs_file)
    possible_red_flags_df, red_flags_df = red_flags.identify_red_flags(
        target.stem, top_source_scores, refs, threshold=0.1
    )

    write_df_to_s3(
        red_flags_df,
        Path(f"Modal/out/red_flags/{source.stem}_{target.stem}/red_flags.csv"),
    )
    write_df_to_s3(
        possible_red_flags_df,
        Path(
            Key=f"Modal/out/red_flags/{source.stem}_{target.stem}/possible_red_flags.csv"
        ),
    )


def create_threshold_scores(
    source, target, outpath, threshold, is_bible: bool = True, refresh: bool = False
):
    if (
        does_file_exist(
            Path(
                f"Modal/out/threshold_scores/{source.stem}_{target.stem}/threshold_scores.csv"
            )
        )
        and not refresh
    ):
        return
    s3 = boto3.client("s3")
    total_scores_file = Path("total_scores.csv")
    s3.download_file(
        "aqua-word-alignment",
        f"Modal/out/total_scores/{source.stem}_{target.stem}/total_scores.csv",
        total_scores_file,
    )
    total_scores = pd.read_csv(total_scores_file)
    df = threshold_scores.get_threshold_scores(total_scores, threshold=threshold)
    write_df_to_s3(
        df,
        Path(
            f"Modal/out/threshold_scores/{source.stem}_{target.stem}/threshold_scores.csv"
        ),
    )
