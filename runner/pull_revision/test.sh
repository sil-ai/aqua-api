#!/bin/bash

export MODAL_TEST=TRUE

modal deploy pull_revision.py

pytest pull_revision_test.py