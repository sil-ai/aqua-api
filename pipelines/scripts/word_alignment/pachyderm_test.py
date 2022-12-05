from pathlib import Path
import pytest

import pachyderm

@pytest.mark.parametrize("data_dir,ref_dir,outpath", [
                                                    (Path("fixtures/data_dir"), Path("fixtures/ref_dir"),Path('fixtures/out')), 
                                                    ])
def test_run_pachyderm(data_dir, ref_dir, outpath):
    pachyderm.run_pachyderm(data_dir=data_dir, ref_dir=ref_dir, outpath=outpath)
