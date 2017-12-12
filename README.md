# Trend_Analysis

A tool for trend analysis of Next Generation Sequencing quality data.

## Getting Started 
The tool requires the following dependencies:
```
SQLAlchemy
PyMySQL
PyVCF
```

## Running the tool
There are different options with this tool.
Data can be uploaded, deleted or updated.

### To upload data
Raw run data:
```
python trend_analysis.py upload raw_data 'run' 'path' 'sequencer'
```
Processed run data:
```
python trend_analysis.py upload processed_data 'run' 'path'
```
Processed sample data:
```
python trend_analysis.py upload sample_processed 'run' 'path' 'samples'
```

### To delete data
All run data:
```
python trend_analysis.py delete run_all 'run'
```
Raw run data:
```
python trend_analysis.py delete raw_run 'run'
```
Processed run data:
```
python trend_analysis.py delete run_proc 'run'
```
Processed sample data:
```
python trend_analysis.py delete sample_proc 'run' 'samples'
```

### To update data
Delete and then update all run data:
```
python trend_analysis.py update run_all 'run' 'path_raw' 'path_proc' 'sequencer'
```
Delete and then update processed sample data:
```
python trend_analysis.py update sample __'run'__ 'path' 'samples'
```
