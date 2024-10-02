from latch.resources.workflow import workflow
from latch.types import LatchFile
from latch.types.metadata import (
    LatchAuthor, LatchMetadata, LatchParameter, LatchRule
)

from wf.task import ff_task
from wf.utils import Genome, OutputType


metadata = LatchMetadata(
    display_name="fragment fixer",
    author=LatchAuthor(
        name="James McGann",
        email="jamesm@atlasxomics.com",
        github="github.com/atlasxomics"
    ),
    parameters={
        "input_file": LatchParameter(
            display_name="input file",
            description="""bed/bed-like file output from Chromap or a similar
                alignment tool (i.e., aln.bed, fragments.tsv.gz).  File
                extension should end in '.bed' or .tsv.gz'.
                """,
            batch_table_column=True,
        ),
        "run_id": LatchParameter(
            display_name="run id/output file prefix",
            description="Text to apend to the beginning of the output file \
                name.",
            batch_table_column=True,
            rules=[
                LatchRule(
                    regex="^[^/][^-.\s/]+$",
                    message="Prefix cannot start with or contain a '/', dash \
                        (-), a period (.) or space"
                ),
            ]
        ),
        "genome": LatchParameter(
            display_name="genome",
            description="Reference genome for used to make input file.",
            batch_table_column=True,
        ),
        "output_type": LatchParameter(
            display_name="output file type",
            description="Choose whether the Workflow outputs an aln.bed or \
                fragments.tsv.gz file.",
            batch_table_column=True,
        ),
        "output_dir": LatchParameter(
            display_name="output directory",
            description="Name of output directory, in ff_outs/",
            batch_table_column=True,
            rules=[
                LatchRule(
                    regex="^[^/][^-.\s/]+$",
                    message="Output directory cannot start with or contain a \
                        '/', dash  (-), a period (.) or space"
                ),
            ]
        ),
    },
)


@workflow(metadata)
def ff_wf(
    input_file: LatchFile,
    run_id: str,
    genome: Genome,
    output_type: OutputType,
    output_dir: str
) -> None:

    return ff_task(
        input_file=input_file,
        run_id=run_id,
        genome=genome,
        output_type=output_type,
        output_dir=output_dir
    )
