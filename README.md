SmokedDuck Benchmarking Scripts

1. sh setup.sh 

2. Logical and Baseline evaluation: 
  a. git repository of duckdb without any instrumentation: https://anonymous.4open.science/r/duckdb-1853
  b. build:  sudo BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_PYTHON=1 make; cd tools/pythonpkg; sudo python3 setup.py install
  c. sh logical.sh

3. SmokedDuck evaluation:
  a. git repository of instrumented duckdb: https://anonymous.4open.science/r/duckdb-4BE5
  b. build:  sudo BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_LINEAGE=1 BUILD_PYTHON=1 make; cd tools/pythonpkg; sudo python3 setup.py install
  c. sh smokedduck.sh

4. Plot the results: 
  a. python3 plotting/micro.py
  b. python3 plotting/tpch_capture.py
