#!/bin/bash

modal deploy ../../assessments/dummy/dummy.py

MODAL_TEST=TRUE modal deploy runner.py

pytest
