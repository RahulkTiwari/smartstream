#!/bin/bash -x
cd /home/udm/
source QAairflowsandbox/bin/activate
cd /home/udm/PROD/FileInterceptor
python3 RefinitiveFileInterceptor.py "/home/udm/PROD/FileInterceptor/RefinitiveFIconfig.json"
