import logging
import os
import pandas as pd
import subprocess

from latch.resources.tasks import custom_task
from latch.types import LatchDir, LatchFile

from wf.utils import chromsize_paths, Genome, load_chromsizes, OutputType


logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.WARNING,
    handlers=[
        logging.FileHandler("wf.log"),
        logging.StreamHandler()
    ]
)


def call_oob(row: pd.Series, chromsizes: dict) -> bool:

    chrom, start, end = row["chrom"], row["start"], row["end"]
    if chrom in chromsizes:
        chrom_length = chromsizes[chrom]
        if start > chrom_length or end > chrom_length:
            logging.warning(
                f"Found one beyond {chrom=}, {chrom_length=}: \
                {(chrom, start, end)}"
            )
            return False
        else:
            return True
    else:
        logging.warning(f"Wrong chromosome found: {(chrom, start, end)}")
        return False


def filter_oob(input_file: LatchFile, chromsizes: dict) -> pd.DataFrame:
    """Read in the input file as a DataFrame and apply call_oob() to remove any
    fragments with start/stop greater than its corresponding chromosome sizes.
    """

    columns = ["chrom", "start", "end", "cellBarcode", "duplicates"]
    df = pd.read_csv(input_file, sep="\t", header=None, names=columns)

    condition = df.apply(lambda row: call_oob(row, chromsizes), axis=1)

    return df[condition]


def save_frags(
    fragments_df: pd.DataFrame, output_type: str, output_prefix: str
) -> str:

    if output_type == "aln.bed":

        file_name = f"{output_prefix}_ff_aln.bed"
        fragments_df.to_csv(file_name, sep='\t', header=False, index=False)

    elif output_type == "fragments.tsv.gz":

        file_name = f"{output_prefix}_ff_fragments.tsv.gz"
        fragments_df["cellBarcode"] = fragments_df.loc[:, "cellBarcode"].apply(
            lambda x: x + "-1"
        )
        fragments_df.to_csv(
            "fragments.tsv", sep='\t', header=False, index=False
        )

        with open(file_name, "w") as f:
            subprocess.run(["bgzip", "-c", "fragments.tsv"], stdout=f)

    return file_name


@custom_task(cpu=32, memory=384, storage_gib=4949)
def ff_task(
    input_file: LatchFile,
    run_id: str,
    genome: Genome,
    output_type: OutputType,
    output_dir: str
) -> LatchDir:

    logging.info("Filtering OOB...")
    filtered_df = filter_oob(
        input_file.local_path,
        load_chromsizes(chromsize_paths[genome.value])
    )

    logging.info("Saving new fragment files...")
    output_filename = save_frags(filtered_df, output_type.value, run_id)

    os.makedirs(output_dir, exist_ok=True)
    subprocess.run(["mv", "wf.log", output_filename, output_dir])

    return LatchDir(output_dir, f"latch:///ff_outs/{output_dir}")
