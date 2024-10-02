from enum import Enum


chromsize_paths = {
    "hg38":  "chrom_sizes/hg38.chrom.sizes",
    "mm10": "chrom_sizes/mm10.chrom.sizes",
    "rnor6": "chrom_sizes/rn6.chrom.sizes"
}


class Genome(Enum):
    mm10 = "mm10"
    hg38 = "hg38"
    rnor6 = "rnor6"


class OutputType(Enum):
    fragments = "fragments.tsv.gz"
    aln = "aln.bed"


def load_chromsizes(path: str) -> dict:

    sizes = {}
    with open(path, 'r') as f:
        for line in f:
            chrom, length = line.strip().split()
            sizes[chrom] = int(length)

    return sizes
