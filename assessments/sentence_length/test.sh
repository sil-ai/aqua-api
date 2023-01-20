#!/bin/bash

cd ../../runner/push_results/

MODAL_TEST=TRUE modal deploy app.py

cd ../../assessments/sentence_length

MODAL_TEST=TRUE modal deploy sentence_length.py

pytest sentence_length_test.py
