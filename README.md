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
The raw run data is run data directly from the sequencer, while processed data comes from the pipeline. 

### Upload data
Needed for data uploading is the run name, the path to the directory of the run and the sequencer.  
For the raw run data the path needed is to the raw run directory, for the processed data to the processed data directory.  
When not all samples of a run need to be uploaded, these samples need to be enter subsequently with the path to their processed run directory.  

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

### Delete data
To delete data from the database only the run name is needed.  
When only certain samples need to be deleted, they need to be enter subsequently with their run name.  

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

### Update data
When data in the datebase needs to be update the data will be delete first before the new data is added.  
To update all data of a run, the run name, path to the raw data directory, path the processed data directory and the sequencer are needed.  
When only the processed data of a run needs to be updated, the run name and the path to the processed data directory are nedded.  
In order to update certain samples, the run name, the path to the procesed data and the samples need to be entered.  

Delete and then upload new run data:
```
python trend_analysis.py update run_all 'run' 'path_raw' 'path_proc' 'sequencer'
```
Delete and then upload processed run data:
```
python trend_analysis.py update processed_data 'run'
```
Delete and then upload processed sample data:
```
python trend_analysis.py update sample 'run' 'path' 'samples'
```

## Updating the tool
There are multiple scenarios where the codes need to be changed.  
When a new sequencer will be used, this sequencer needs to be added to the script.  
At the same time, when there are quality parameters added to one of the files, these new parameters also need to be added to the script.  

#### New sequencer
* config.py: add the new sequencer to the list Sequencers
* R-script: add the new sequencer with a corresponding colour and shape

#### New parameters
* Python scripts:

  * import_data: add the parameter to the corresponding dictionary
  * upload/file: add the parameter to the insert

* MySQL database: add the parameter to the corresponding table
