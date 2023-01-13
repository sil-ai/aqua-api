#!/bin/bash

MODAL_TEST=TRUE modal deploy assess.py

pytest assess_test.py
