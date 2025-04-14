import math
import os
from typing import Literal, Optional, Union
import modal
from modal import app
from pydantic import BaseModel

# Manage deployment suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix += "-test"



cache_vol = modal.Volume.from_name("sem-sim-labse-model-cache", create_if_missing=True)
CACHE_PATH = "/root/model_cache"

image_envs = {k: v for k, v in os.environ.items() if k.startswith("MODAL_")}

app = modal.App(
    "semantic-similarity" + suffix,
    image=modal.Image.debian_slim()
    .pip_install(
        "pandas~=1.5.0", "torch~=2.1.0", "transformers~=4.34.0", "tqdm~=4.66.0"
    )
    .copy_mount(
        modal.Mount.from_local_file(
            local_path="../../fixtures/vref.txt", remote_path="/root/vref.txt"
        )
    )
    .copy_mount(
        modal.Mount.from_local_file(
            local_path="merge_revision.py", remote_path="/root/merge_revision.py"
        )
    )
    .env(image_envs),
)

run_pull_rev = modal.Function.lookup("pull-revision" + suffix, "pull_revision")


class Assessment(BaseModel):
    id: Optional[int] = None
    revision_id: int
    reference_id: int
    type: Literal["semantic-similarity"]


@app.function(
    timeout=7200,
    secrets=[modal.Secret.from_dict({"TRANSFORMERS_CACHE": CACHE_PATH})],
    volumes={CACHE_PATH: cache_vol},
)
def get_labse_model(cache_path=CACHE_PATH):
    from transformers import BertTokenizerFast, BertModel

    try:
        semsim_model = BertModel.from_pretrained(
            "setu4993/LaBSE", cache_dir=cache_path
        ).eval()
    except OSError as e:
        print(e)
        print("Downloading model instead of using cache...")
        semsim_model = BertModel.from_pretrained(
            "setu4993/LaBSE", cache_dir=cache_path, force_download=True
        ).eval()

    try:
        semsim_tokenizer = BertTokenizerFast.from_pretrained(
            "setu4993/LaBSE", cache_dir=cache_path
        )
    except OSError as e:
        print(e)
        print("Downloading tokenizer instead of using cache...")
        semsim_tokenizer = BertTokenizerFast.from_pretrained(
            "setu4993/LaBSE", cache_dir=cache_path, force_download=True
        )

    return semsim_model, semsim_tokenizer


@app.function(timeout=600, retries=3, gpu='T4')
def get_sim_scores(
    batched_sentences,
    semsim_model=None,
    semsim_tokenizer=None,
):
    import torch
    import gc
    from torch.cuda import OutOfMemoryError

    rev_sents_input, ref_sents_input = batched_sentences
    batch_size = len(rev_sents_input)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    if semsim_model is None or semsim_tokenizer is None:
        semsim_model, semsim_tokenizer = get_labse_model.remote()

    semsim_model = semsim_model.to(device)
    semsim_model.eval()

    try:
        rev_sents_input_tokenized = semsim_tokenizer(
            rev_sents_input, return_tensors="pt", padding=True, truncation=True
        ).to(device)
        ref_sents_input_tokenized = semsim_tokenizer(
            ref_sents_input, return_tensors="pt", padding=True, truncation=True
        ).to(device)

        with torch.no_grad():
            rev_sents_output = semsim_model(**rev_sents_input_tokenized)
            ref_sents_output = semsim_model(**ref_sents_input_tokenized)

        rev_sents_embedding = rev_sents_output.pooler_output
        ref_sents_embedding = ref_sents_output.pooler_output

        sim_scores = torch.nn.CosineSimilarity(dim=1, eps=1e-6)(
            rev_sents_embedding, ref_sents_embedding
        ).tolist()

        return sim_scores

    except OutOfMemoryError as e:
        print(f"OutOfMemoryError occurred: {e}")
        
        # Free GPU memory
        if 'rev_sents_input_tokenized' in locals():
            del rev_sents_input_tokenized
        if 'ref_sents_input_tokenized' in locals():
            del ref_sents_input_tokenized
        if 'rev_sents_output' in locals():
            del rev_sents_output
        if 'ref_sents_output' in locals():
            del ref_sents_output
        if 'rev_sents_embedding' in locals():
            del rev_sents_embedding
        if 'ref_sents_embedding' in locals():
            del ref_sents_embedding

        gc.collect()  # Force garbage collection
        torch.cuda.empty_cache()  # Clear the CUDA memory cache
        torch.cuda.synchronize()  # Ensure all GPU operations are complete
        print("Cleared GPU memory. Splitting batch...")

        # Split and retry
        if batch_size > 2:  # Don't keep splitting if batch is too small
            mid_point = batch_size // 2
            print(f"Splitting batch, new size: {mid_point} and {batch_size - mid_point}...")
            first_half = (rev_sents_input[:mid_point], ref_sents_input[:mid_point])
            second_half = (rev_sents_input[mid_point:], ref_sents_input[mid_point:])
            
            # Process each half
            first_half_scores = get_sim_scores.remote(first_half, semsim_model, semsim_tokenizer)
            second_half_scores = get_sim_scores.remote(second_half, semsim_model, semsim_tokenizer)
            
            # Combine results
            return first_half_scores + second_half_scores
        else:
            print("Unable to process batch even after splitting. Returning dummy scores...")
            print(f"Sentences: {rev_sents_input=} /n {ref_sents_input=}")
            return [0.5] * batch_size

    finally:
        # Ensure GPU memory is cleared after processing
        gc.collect()
        torch.cuda.empty_cache()
        torch.cuda.synchronize()


@app.function()
def get_text(rev_id: int, AQUA_DB: str):
    return run_pull_rev.remote(rev_id, AQUA_DB)


@app.function()
def merge(revision_id, revision_verses, reference_id, reference_verses):
    from merge_revision import MergeRevision

    mr = MergeRevision(revision_id, revision_verses, reference_id, reference_verses)
    return mr.merge_revision()


@app.function(timeout=7200, volumes={CACHE_PATH: cache_vol})
def assess(assessment: Union[Assessment, dict], AQUA_DB: str, **kwargs):
    from tqdm import tqdm

    if isinstance(assessment, dict):
        assessment = Assessment(**assessment)
    revision = get_text.remote(assessment.revision_id, AQUA_DB)
    reference = get_text.remote(assessment.reference_id, AQUA_DB)
    df = merge.remote(
        assessment.revision_id, revision, assessment.reference_id, reference
    )

    batch_size = 512
    rev_sents = df["revision"].to_list()
    ref_sents = df["reference"].to_list()
    vrefs = df.index.to_list()
    assessment_id = [assessment.id] * len(vrefs)
    rev_sents_batched = [
        rev_sents[i : i + batch_size] for i in range(0, len(rev_sents), batch_size)
    ]
    ref_sents_batched = [
        ref_sents[i : i + batch_size] for i in range(0, len(ref_sents), batch_size)
    ]
    # semsim_model, semsim_tokenizer = get_labse_model.remote()
    sim_scores = tqdm(
        get_sim_scores.map(
            zip(rev_sents_batched, ref_sents_batched),
            # kwargs={"semsim_model": semsim_model, "semsim_tokenizer": semsim_tokenizer},
        )
    )

    sim_scores = [item for sublist in sim_scores for item in sublist]

    results = [
        {
            "assessment_id": assessment_id[j],
            "vref": vrefs[j],
            "score": sim_scores[j] if not math.isnan(sim_scores[j]) else 0,
        }
        for j in range(len(vrefs))
    ]

    print(results[:20])

    return {"results": results}
