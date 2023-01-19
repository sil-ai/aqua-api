#!/bin/bash

MODAL_TEST=TRUE modal deploy sentence_length.py

pytest sentence_length_test.py
