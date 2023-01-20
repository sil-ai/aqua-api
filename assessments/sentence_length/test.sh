#!/bin/bash

MODAL_TEST=TRUE modal deploy app.py

pytest sentence_length_test.py
