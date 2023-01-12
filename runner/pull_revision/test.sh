#!/bin/bash

export MODAL_TEST=TRUE

modal deploy app.py

pytest app_test.py

# Should we then un-deploy?
