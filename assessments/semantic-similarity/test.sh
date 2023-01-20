#!/bin/bash

#don't need a lot of containers for small test
MODAL_TEST=TRUE CONCURRENCY_LIMIT=2 modal deploy assess.py

pytest -sv 