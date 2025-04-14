#!/bin/bash

MODAL_TEST=TRUE modal deploy app.py

pytest app_test.py
