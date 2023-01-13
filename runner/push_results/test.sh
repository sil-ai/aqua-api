#!/bin/bash

export MODAL_TEST=TRUE

modal deploy push_results.py

pytest
