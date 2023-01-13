#!/bin/bash

export MODAL_TEST=TRUE

modal deploy app.py

pytest
