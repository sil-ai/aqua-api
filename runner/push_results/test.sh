#!/bin/bash

export MODAL_TEST=TRUE

modal deploy push_results.py

python push_results_test.py

# Should we then un-deploy?
